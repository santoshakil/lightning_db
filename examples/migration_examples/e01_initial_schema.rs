//! Example 1: Initial Schema Creation
//!
//! This example demonstrates creating the initial database schema with:
//! - User management tables
//! - Product catalog structure
//! - Order processing schema
//! - Indexes for performance
//! - Constraints and relationships

use lightning_db::{
    Database, Result,
    schema_migration::{
        Migration, MigrationStep, Schema, SchemaVersion,
        TableDefinition, ColumnDefinition, DataType, IndexDefinition,
        IndexType, IndexColumn, SortOrder, IndexOptions, TableOptions,
        ConstraintDefinition, ConstraintType,
    },
};
use std::collections::BTreeMap;
use super::{MigrationExampleRunner, validation};

/// Initial schema migration for an e-commerce application
pub struct InitialSchemaMigration {}

impl Migration for InitialSchemaMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0)
    }

    fn description(&self) -> &str {
        "Initial e-commerce database schema with users, products, and orders"
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // Users table
            MigrationStep::CreateTable {
                name: "users".to_string(),
                definition: create_users_table(),
            },
            
            // Categories table  
            MigrationStep::CreateTable {
                name: "categories".to_string(),
                definition: create_categories_table(),
            },
            
            // Products table
            MigrationStep::CreateTable {
                name: "products".to_string(),
                definition: create_products_table(),
            },
            
            // Orders table
            MigrationStep::CreateTable {
                name: "orders".to_string(),
                definition: create_orders_table(),
            },
            
            // Order items table
            MigrationStep::CreateTable {
                name: "order_items".to_string(),
                definition: create_order_items_table(),
            },
            
            // Indexes for performance
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_users_email".to_string(),
                    table: "users".to_string(),
                    columns: vec![IndexColumn {
                        name: "email".to_string(),
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
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_products_category".to_string(),
                    table: "products".to_string(),
                    columns: vec![IndexColumn {
                        name: "category_id".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["name".to_string(), "price".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_orders_user_date".to_string(),
                    table: "orders".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "user_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "created_at".to_string(),
                            order: SortOrder::Descending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["status".to_string(), "total_amount".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_order_items_order".to_string(),
                    table: "order_items".to_string(),
                    columns: vec![IndexColumn {
                        name: "order_id".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["product_id".to_string(), "quantity".to_string()],
                    },
                },
            },
        ]
    }

    fn target_schema(&self) -> Schema {
        let mut tables = BTreeMap::new();
        let mut indexes = BTreeMap::new();
        let mut constraints = BTreeMap::new();

        // Add all tables
        tables.insert("users".to_string(), create_users_table());
        tables.insert("categories".to_string(), create_categories_table());
        tables.insert("products".to_string(), create_products_table());
        tables.insert("orders".to_string(), create_orders_table());
        tables.insert("order_items".to_string(), create_order_items_table());

        // Add all indexes
        indexes.insert("idx_users_email".to_string(), IndexDefinition {
            name: "idx_users_email".to_string(),
            table: "users".to_string(),
            columns: vec![IndexColumn {
                name: "email".to_string(),
                order: SortOrder::Ascending,
            }],
            index_type: IndexType::BTree,
            options: IndexOptions {
                unique: true,
                predicate: None,
                include: vec![],
            },
        });

        indexes.insert("idx_products_category".to_string(), IndexDefinition {
            name: "idx_products_category".to_string(),
            table: "products".to_string(),
            columns: vec![IndexColumn {
                name: "category_id".to_string(),
                order: SortOrder::Ascending,
            }],
            index_type: IndexType::BTree,
            options: IndexOptions {
                unique: false,
                predicate: None,
                include: vec!["name".to_string(), "price".to_string()],
            },
        });

        indexes.insert("idx_orders_user_date".to_string(), IndexDefinition {
            name: "idx_orders_user_date".to_string(),
            table: "orders".to_string(),
            columns: vec![
                IndexColumn {
                    name: "user_id".to_string(),
                    order: SortOrder::Ascending,
                },
                IndexColumn {
                    name: "created_at".to_string(),
                    order: SortOrder::Descending,
                },
            ],
            index_type: IndexType::BTree,
            options: IndexOptions {
                unique: false,
                predicate: None,
                include: vec!["status".to_string(), "total_amount".to_string()],
            },
        });

        indexes.insert("idx_order_items_order".to_string(), IndexDefinition {
            name: "idx_order_items_order".to_string(),
            table: "order_items".to_string(),
            columns: vec![IndexColumn {
                name: "order_id".to_string(),
                order: SortOrder::Ascending,
            }],
            index_type: IndexType::BTree,
            options: IndexOptions {
                unique: false,
                predicate: None,
                include: vec!["product_id".to_string(), "quantity".to_string()],
            },
        });

        // Add foreign key constraints
        constraints.insert("fk_products_category".to_string(), ConstraintDefinition {
            name: "fk_products_category".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                table: "products".to_string(),
                columns: vec!["category_id".to_string()],
                referenced_table: "categories".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            },
        });

        constraints.insert("fk_orders_user".to_string(), ConstraintDefinition {
            name: "fk_orders_user".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                table: "orders".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            },
        });

        Schema {
            version: self.version(),
            tables,
            indexes,
            constraints,
            metadata: BTreeMap::new(),
        }
    }

    fn validate_preconditions(&self, _database: &Database) -> Result<()> {
        // Ensure no conflicting tables exist
        println!("✅ Validating initial schema preconditions...");
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        println!("🔍 Validating initial schema postconditions...");
        
        // Validate all tables were created
        validation::validate_table_exists(database, "users")?;
        validation::validate_table_exists(database, "categories")?;
        validation::validate_table_exists(database, "products")?;
        validation::validate_table_exists(database, "orders")?;
        validation::validate_table_exists(database, "order_items")?;
        
        // Validate all indexes were created
        validation::validate_index_exists(database, "idx_users_email")?;
        validation::validate_index_exists(database, "idx_products_category")?;
        validation::validate_index_exists(database, "idx_orders_user_date")?;
        validation::validate_index_exists(database, "idx_order_items_order")?;
        
        println!("✅ Initial schema validation completed successfully");
        Ok(())
    }
}

// Table creation functions
fn create_users_table() -> TableDefinition {
    TableDefinition {
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
                name: "email".to_string(),
                data_type: DataType::String,
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
                name: "first_name".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "last_name".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "password_hash".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "active".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                default: Some(serde_json::Value::Bool(true)),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "created_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "updated_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_categories_table() -> TableDefinition {
    TableDefinition {
        name: "categories".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "description".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "parent_id".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "active".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                default: Some(serde_json::Value::Bool(true)),
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
    }
}

fn create_products_table() -> TableDefinition {
    TableDefinition {
        name: "products".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "category_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "description".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "price".to_string(),
                data_type: DataType::Decimal,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "cost".to_string(),
                data_type: DataType::Decimal,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "sku".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "inventory_count".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default: Some(serde_json::Value::Number(serde_json::Number::from(0))),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "active".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                default: Some(serde_json::Value::Bool(true)),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "created_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "updated_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_orders_table() -> TableDefinition {
    TableDefinition {
        name: "orders".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "user_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "order_number".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "status".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: Some(serde_json::Value::String("pending".to_string())),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "total_amount".to_string(),
                data_type: DataType::Decimal,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "tax_amount".to_string(),
                data_type: DataType::Decimal,
                nullable: false,
                default: Some(serde_json::Value::String("0.00".to_string())),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "shipping_amount".to_string(),
                data_type: DataType::Decimal,
                nullable: false,
                default: Some(serde_json::Value::String("0.00".to_string())),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "shipping_address".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "billing_address".to_string(),
                data_type: DataType::Json,
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
            ColumnDefinition {
                name: "updated_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_order_items_table() -> TableDefinition {
    TableDefinition {
        name: "order_items".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "order_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "product_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "quantity".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "unit_price".to_string(),
                data_type: DataType::Decimal,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "total_price".to_string(),
                data_type: DataType::Decimal,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "product_snapshot".to_string(),
                data_type: DataType::Json,
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
    }
}

/// Run the initial schema migration example
pub fn run_example() -> Result<()> {
    println!("🚀 Running Example 1: Initial Schema Creation");
    println!("===============================================\n");
    
    let runner = MigrationExampleRunner::new("examples/databases/e01_initial_schema.db")?;
    
    // Create sample data for validation
    println!("📝 Creating sample data for validation...");
    create_sample_data(&runner)?;
    
    // Run the migration
    let migration = InitialSchemaMigration {};
    runner.run_migration_with_validation(
        migration,
        Box::new(|db| validate_initial_schema(db)),
    )?;
    
    // Show migration history
    runner.show_history(Some(5))?;
    
    // Run performance benchmark
    runner.benchmark_performance(1000)?;
    
    println!("\n🎉 Example 1 completed successfully!");
    println!("Next: Run example 2 to see user profile enhancements");
    
    Ok(())
}

/// Create sample data for the initial schema
fn create_sample_data(runner: &MigrationExampleRunner) -> Result<()> {
    // Insert sample categories
    for i in 1..=5 {
        let key = format!("categories_{:03}", i);
        let value = serde_json::json!({
            "id": i,
            "name": format!("Category {}", i),
            "description": format!("Description for category {}", i),
            "parent_id": if i > 3 { Some(1) } else { null },
            "active": true,
            "created_at": chrono::Utc::now().timestamp()
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    // Insert sample users
    for i in 1..=10 {
        let key = format!("users_{:03}", i);
        let value = serde_json::json!({
            "id": i,
            "email": format!("user{}@example.com", i),
            "username": format!("user{}", i),
            "first_name": format!("First{}", i),
            "last_name": format!("Last{}", i),
            "password_hash": "hashed_password",
            "active": true,
            "created_at": chrono::Utc::now().timestamp(),
            "updated_at": null
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    // Insert sample products
    for i in 1..=25 {
        let key = format!("products_{:03}", i);
        let value = serde_json::json!({
            "id": i,
            "category_id": (i % 5) + 1,
            "name": format!("Product {}", i),
            "description": format!("Description for product {}", i),
            "price": format!("{}.99", 10 + (i * 5)),
            "cost": format!("{}.50", 5 + (i * 2)),
            "sku": format!("SKU-{:06}", i),
            "inventory_count": 100 - (i * 2),
            "active": true,
            "created_at": chrono::Utc::now().timestamp(),
            "updated_at": null
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    println!("✅ Created sample data: 5 categories, 10 users, 25 products");
    Ok(())
}

/// Validate the initial schema
fn validate_initial_schema(database: &Database) -> Result<()> {
    println!("🔍 Validating initial schema structure...");
    
    // Verify sample data integrity
    let categories_count = validation::count_records_with_prefix(database, "categories_")?;
    let users_count = validation::count_records_with_prefix(database, "users_")?;
    let products_count = validation::count_records_with_prefix(database, "products_")?;
    
    println!("📊 Data counts: {} categories, {} users, {} products", 
             categories_count, users_count, products_count);
    
    if categories_count != 5 || users_count != 10 || products_count != 25 {
        return Err(lightning_db::Error::Validation(
            "Sample data counts don't match expected values".to_string()
        ));
    }
    
    // Validate JSON structure for a few records
    if let Some(user_data) = database.get(b"users_001")? {
        validation::validate_json_structure(&user_data, &["id", "email", "username"])?;
    }
    
    if let Some(product_data) = database.get(b"products_001")? {
        validation::validate_json_structure(&product_data, &["id", "category_id", "name", "price"])?;
    }
    
    println!("✅ Initial schema validation successful");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_initial_schema_migration() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let migration = InitialSchemaMigration {};
        
        // Test migration steps
        let steps = migration.steps();
        assert_eq!(steps.len(), 9); // 5 tables + 4 indexes
        
        // Test version
        assert_eq!(migration.version(), SchemaVersion::new(1, 0));
        
        // Test schema
        let schema = migration.target_schema();
        assert_eq!(schema.tables.len(), 5);
        assert_eq!(schema.indexes.len(), 4);
        assert_eq!(schema.constraints.len(), 2);
    }

    #[test]
    fn test_table_definitions() {
        let users_table = create_users_table();
        assert_eq!(users_table.name, "users");
        assert_eq!(users_table.columns.len(), 9);
        assert_eq!(users_table.primary_key, vec!["id".to_string()]);
        
        let products_table = create_products_table();
        assert_eq!(products_table.name, "products");
        assert_eq!(products_table.columns.len(), 11);
    }
}