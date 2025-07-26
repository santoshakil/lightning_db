//! Initial Database Schema Migration
//! 
//! This template provides a starting point for your first migration.
//! Edit this file to define your initial database schema.

use lightning_db::schema_migration::{
    Migration, MigrationStep, Schema, SchemaVersion,
    TableDefinition, ColumnDefinition, DataType, IndexDefinition,
    IndexType, IndexColumn, SortOrder, IndexOptions, TableOptions,
    Result,
};
use lightning_db::Database;
use std::collections::BTreeMap;

pub struct InitialSchemaMigration {}

impl Migration for InitialSchemaMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0)
    }

    fn description(&self) -> &str {
        "Initial database schema"
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // Example: Create a users table
            MigrationStep::CreateTable {
                name: "users".to_string(),
                definition: TableDefinition {
                    name: "users".to_string(),
                    columns: vec![
                        ColumnDefinition {
                            name: "id".to_string(),
                            data_type: DataType::Int64,
                            nullable: false,
                            default: None,
                            constraints: vec![],
                        },
                        ColumnDefinition {
                            name: "username".to_string(),
                            data_type: DataType::String,
                            nullable: false,
                            default: None,
                            constraints: vec![],
                        },
                        ColumnDefinition {
                            name: "email".to_string(),
                            data_type: DataType::String,
                            nullable: true,
                            default: None,
                            constraints: vec![],
                        },
                        ColumnDefinition {
                            name: "created_at".to_string(),
                            data_type: DataType::Timestamp,
                            nullable: false,
                            default: None,
                            constraints: vec![],
                        },
                    ],
                    primary_key: vec!["id".to_string()],
                    options: TableOptions::default(),
                },
            },

            // Example: Create an index on username
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_users_username".to_string(),
                    table: "users".to_string(),
                    columns: vec![IndexColumn {
                        name: "username".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: true,
                        predicate: None,
                        include: vec![],
                    },
                },
            },
        ]
    }

    fn target_schema(&self) -> Schema {
        let mut tables = BTreeMap::new();
        let mut indexes = BTreeMap::new();

        // Define the target schema after this migration
        tables.insert("users".to_string(), TableDefinition {
            name: "users".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    default: None,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: "username".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                    default: None,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: "email".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: "created_at".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: false,
                    default: None,
                    constraints: vec![],
                },
            ],
            primary_key: vec!["id".to_string()],
            options: TableOptions::default(),
        });

        indexes.insert("idx_users_username".to_string(), IndexDefinition {
            name: "idx_users_username".to_string(),
            table: "users".to_string(),
            columns: vec![IndexColumn {
                name: "username".to_string(),
                order: SortOrder::Ascending,
            }],
            index_type: IndexType::BTree,
            options: IndexOptions {
                unique: true,
                predicate: None,
                include: vec![],
            },
        });

        Schema {
            version: self.version(),
            tables,
            indexes,
            constraints: BTreeMap::new(),
            metadata: BTreeMap::new(),
        }
    }

    fn validate_preconditions(&self, _database: &Database) -> Result<()> {
        // Pre-migration validation
        // Example: Check that no conflicting tables exist
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        // Post-migration validation  
        // Example: Verify that the users table was created
        let table_key = b"__table_def_users";
        if database.get(table_key)?.is_none() {
            return Err(crate::Error::Validation(
                "Users table was not created successfully".to_string()
            ));
        }
        Ok(())
    }
}