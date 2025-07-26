//! Example 2: Adding User Profiles
//!
//! This example demonstrates schema evolution by adding:
//! - User profile table with extended information
//! - User preferences and settings
//! - Profile image and metadata storage
//! - Relationships and additional indexes
//! - Data migration from existing users

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

/// User profiles migration - extends the initial schema
pub struct AddUserProfilesMigration {}

impl Migration for AddUserProfilesMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 1)
    }

    fn description(&self) -> &str {
        "Add comprehensive user profiles with preferences and social features"
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // User profiles table
            MigrationStep::CreateTable {
                name: "user_profiles".to_string(),
                definition: create_user_profiles_table(),
            },
            
            // User preferences table
            MigrationStep::CreateTable {
                name: "user_preferences".to_string(),
                definition: create_user_preferences_table(),
            },
            
            // User sessions table for better auth tracking
            MigrationStep::CreateTable {
                name: "user_sessions".to_string(),
                definition: create_user_sessions_table(),
            },
            
            // User activity log
            MigrationStep::CreateTable {
                name: "user_activity".to_string(),
                definition: create_user_activity_table(),
            },
            
            // Add new columns to existing users table
            MigrationStep::AddColumn {
                table: "users".to_string(),
                column: ColumnDefinition {
                    name: "last_login_at".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            MigrationStep::AddColumn {
                table: "users".to_string(),
                column: ColumnDefinition {
                    name: "email_verified".to_string(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    default: Some(serde_json::Value::Bool(false)),
                    constraints: vec![],
                },
            },
            
            MigrationStep::AddColumn {
                table: "users".to_string(),
                column: ColumnDefinition {
                    name: "timezone".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: Some(serde_json::Value::String("UTC".to_string())),
                    constraints: vec![],
                },
            },
            
            // Indexes for new tables
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_user_profiles_user".to_string(),
                    table: "user_profiles".to_string(),
                    columns: vec![IndexColumn {
                        name: "user_id".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: true,
                        predicate: None,
                        include: vec!["display_name".to_string(), "avatar_url".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_user_sessions_token".to_string(),
                    table: "user_sessions".to_string(),
                    columns: vec![IndexColumn {
                        name: "session_token".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: true,
                        predicate: None,
                        include: vec!["user_id".to_string(), "expires_at".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_user_sessions_user_active".to_string(),
                    table: "user_sessions".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "user_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "active".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["created_at".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_user_activity_user_date".to_string(),
                    table: "user_activity".to_string(),
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
                        include: vec!["action".to_string()],
                    },
                },
            },
            
            // Index on users table for the new columns
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_users_last_login".to_string(),
                    table: "users".to_string(),
                    columns: vec![IndexColumn {
                        name: "last_login_at".to_string(),
                        order: SortOrder::Descending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("last_login_at IS NOT NULL".to_string()),
                        include: vec!["username".to_string(), "active".to_string()],
                    },
                },
            },
        ]
    }

    fn target_schema(&self) -> Schema {
        let mut tables = BTreeMap::new();
        let mut indexes = BTreeMap::new();
        let mut constraints = BTreeMap::new();

        // Include all original tables (modified users table)
        tables.insert("users".to_string(), create_enhanced_users_table());
        tables.insert("categories".to_string(), create_categories_table_from_v1());
        tables.insert("products".to_string(), create_products_table_from_v1());
        tables.insert("orders".to_string(), create_orders_table_from_v1());
        tables.insert("order_items".to_string(), create_order_items_table_from_v1());
        
        // Add new tables
        tables.insert("user_profiles".to_string(), create_user_profiles_table());
        tables.insert("user_preferences".to_string(), create_user_preferences_table());
        tables.insert("user_sessions".to_string(), create_user_sessions_table());
        tables.insert("user_activity".to_string(), create_user_activity_table());

        // Include all original indexes plus new ones
        add_original_indexes(&mut indexes);
        add_new_indexes(&mut indexes);

        // Add constraints
        constraints.insert("fk_user_profiles_user".to_string(), ConstraintDefinition {
            name: "fk_user_profiles_user".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                table: "user_profiles".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: None,
            },
        });

        constraints.insert("fk_user_preferences_user".to_string(), ConstraintDefinition {
            name: "fk_user_preferences_user".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                table: "user_preferences".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: None,
            },
        });

        constraints.insert("fk_user_sessions_user".to_string(), ConstraintDefinition {
            name: "fk_user_sessions_user".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                table: "user_sessions".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
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

    fn validate_preconditions(&self, database: &Database) -> Result<()> {
        println!("✅ Validating user profiles migration preconditions...");
        
        // Ensure users table exists from previous migration
        validation::validate_table_exists(database, "users")?;
        
        // Check that we have some existing users to migrate
        let user_count = validation::count_records_with_prefix(database, "users_")?;
        if user_count == 0 {
            return Err(lightning_db::Error::Validation(
                "No existing users found - initial schema migration required first".to_string()
            ));
        }
        
        println!("✅ Found {} existing users for migration", user_count);
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        println!("🔍 Validating user profiles migration postconditions...");
        
        // Validate new tables were created
        validation::validate_table_exists(database, "user_profiles")?;
        validation::validate_table_exists(database, "user_preferences")?;
        validation::validate_table_exists(database, "user_sessions")?;
        validation::validate_table_exists(database, "user_activity")?;
        
        // Validate new indexes were created
        validation::validate_index_exists(database, "idx_user_profiles_user")?;
        validation::validate_index_exists(database, "idx_user_sessions_token")?;
        validation::validate_index_exists(database, "idx_user_sessions_user_active")?;
        validation::validate_index_exists(database, "idx_user_activity_user_date")?;
        validation::validate_index_exists(database, "idx_users_last_login")?;
        
        // Validate that user profiles were created for existing users
        let user_count = validation::count_records_with_prefix(database, "users_")?;
        let profile_count = validation::count_records_with_prefix(database, "user_profiles_")?;
        
        if profile_count < user_count {
            println!("⚠️  Warning: Only {} profiles created for {} users", profile_count, user_count);
        } else {
            println!("✅ Created {} profiles for {} users", profile_count, user_count);
        }
        
        println!("✅ User profiles migration validation completed successfully");
        Ok(())
    }
}

// New table definitions
fn create_user_profiles_table() -> TableDefinition {
    TableDefinition {
        name: "user_profiles".to_string(),
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
                name: "display_name".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "bio".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "avatar_url".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "cover_image_url".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "location".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "website".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "birth_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "phone_number".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "social_links".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "privacy_settings".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: Some(serde_json::json!({"profile_visible": true, "email_visible": false})),
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

fn create_user_preferences_table() -> TableDefinition {
    TableDefinition {
        name: "user_preferences".to_string(),
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
                name: "theme".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: Some(serde_json::Value::String("light".to_string())),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "language".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: Some(serde_json::Value::String("en".to_string())),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "currency".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: Some(serde_json::Value::String("USD".to_string())),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "email_notifications".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: Some(serde_json::json!({
                    "order_updates": true,
                    "promotions": false,
                    "newsletter": false,
                    "security_alerts": true
                })),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "push_notifications".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: Some(serde_json::json!({
                    "order_updates": true,
                    "promotions": false,
                    "recommendations": true
                })),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "accessibility_settings".to_string(),
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

fn create_user_sessions_table() -> TableDefinition {
    TableDefinition {
        name: "user_sessions".to_string(),
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
                name: "session_token".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "refresh_token".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "device_info".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "ip_address".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "user_agent".to_string(),
                data_type: DataType::String,
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
                name: "expires_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "last_activity_at".to_string(),
                data_type: DataType::Timestamp,
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

fn create_user_activity_table() -> TableDefinition {
    TableDefinition {
        name: "user_activity".to_string(),
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
                name: "action".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "resource_type".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "resource_id".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "metadata".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "ip_address".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "user_agent".to_string(),
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
    }
}

// Enhanced users table with new columns
fn create_enhanced_users_table() -> TableDefinition {
    let mut table = super::e01_initial_schema::create_users_table();
    
    // Add new columns
    table.columns.extend(vec![
        ColumnDefinition {
            name: "last_login_at".to_string(),
            data_type: DataType::Timestamp,
            nullable: true,
            default: None,
            constraints: vec![],
        },
        ColumnDefinition {
            name: "email_verified".to_string(),
            data_type: DataType::Boolean,
            nullable: false,
            default: Some(serde_json::Value::Bool(false)),
            constraints: vec![],
        },
        ColumnDefinition {
            name: "timezone".to_string(),
            data_type: DataType::String,
            nullable: true,
            default: Some(serde_json::Value::String("UTC".to_string())),
            constraints: vec![],
        },
    ]);
    
    table
}

// Helper functions to recreate tables from v1 (would normally import these)
fn create_categories_table_from_v1() -> TableDefinition {
    // This would normally reference the previous migration
    // For now, recreate the definition
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

// Simplified versions of other tables (would import from v1 in real implementation)
fn create_products_table_from_v1() -> TableDefinition {
    TableDefinition {
        name: "products".to_string(),
        columns: vec![], // Would be populated from v1
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_orders_table_from_v1() -> TableDefinition {
    TableDefinition {
        name: "orders".to_string(),
        columns: vec![], // Would be populated from v1
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_order_items_table_from_v1() -> TableDefinition {
    TableDefinition {
        name: "order_items".to_string(),
        columns: vec![], // Would be populated from v1
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn add_original_indexes(indexes: &mut BTreeMap<String, IndexDefinition>) {
    // Add indexes from v1
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
    
    // ... other original indexes would be added here
}

fn add_new_indexes(indexes: &mut BTreeMap<String, IndexDefinition>) {
    indexes.insert("idx_user_profiles_user".to_string(), IndexDefinition {
        name: "idx_user_profiles_user".to_string(),
        table: "user_profiles".to_string(),
        columns: vec![IndexColumn {
            name: "user_id".to_string(),
            order: SortOrder::Ascending,
        }],
        index_type: IndexType::BTree,
        options: IndexOptions {
            unique: true,
            predicate: None,
            include: vec!["display_name".to_string(), "avatar_url".to_string()],
        },
    });
    
    // ... other new indexes
}

/// Run the user profiles migration example
pub fn run_example() -> Result<()> {
    println!("🚀 Running Example 2: Adding User Profiles");
    println!("==========================================\n");
    
    let runner = MigrationExampleRunner::new("examples/databases/e02_user_profiles.db")?;
    
    // First run the initial schema migration
    println!("📋 Setting up initial schema first...");
    let initial_migration = super::e01_initial_schema::InitialSchemaMigration {};
    runner.run_migration_with_validation(
        initial_migration,
        Box::new(|_db| Ok(())),
    )?;
    
    // Create additional sample data before profile migration
    create_additional_users(&runner)?;
    
    // Run the profiles migration
    let migration = AddUserProfilesMigration {};
    runner.run_migration_with_validation(
        migration,
        Box::new(|db| validate_user_profiles_migration(db)),
    )?;
    
    // Create profile data for existing users
    migrate_user_data(&runner)?;
    
    // Show migration history
    runner.show_history(Some(10))?;
    
    // Run performance benchmark
    runner.benchmark_performance(500)?;
    
    println!("\n🎉 Example 2 completed successfully!");
    println!("Next: Run example 3 to see index optimization strategies");
    
    Ok(())
}

/// Create additional users for migration testing
fn create_additional_users(runner: &MigrationExampleRunner) -> Result<()> {
    println!("👥 Creating additional users for profile migration...");
    
    for i in 11..=20 {
        let key = format!("users_{:03}", i);
        let value = serde_json::json!({
            "id": i,
            "email": format!("newuser{}@example.com", i),
            "username": format!("newuser{}", i),
            "first_name": format!("New{}", i),
            "last_name": format!("User{}", i),
            "password_hash": "hashed_password",
            "active": true,
            "created_at": chrono::Utc::now().timestamp(),
            "updated_at": null
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    println!("✅ Created 10 additional users for testing");
    Ok(())
}

/// Migrate existing user data to create profiles
fn migrate_user_data(runner: &MigrationExampleRunner) -> Result<()> {
    println!("🔄 Migrating existing users to create profiles...");
    
    // Scan for existing users
    let scan_result = runner.database.scan(
        Some(b"users_".to_vec()),
        Some(b"users~".to_vec()),
    )?;
    
    let mut migrated_count = 0;
    for item in scan_result {
        let (key, value) = item?;
        let user_data: serde_json::Value = serde_json::from_slice(&value)?;
        
        if let Some(user_id) = user_data.get("id").and_then(|v| v.as_i64()) {
            // Create user profile
            let profile_key = format!("user_profiles_{:03}", user_id);
            let profile_data = serde_json::json!({
                "id": user_id,
                "user_id": user_id,
                "display_name": format!("{} {}", 
                    user_data.get("first_name").and_then(|v| v.as_str()).unwrap_or(""),
                    user_data.get("last_name").and_then(|v| v.as_str()).unwrap_or("")
                ).trim(),
                "bio": format!("User {} profile", user_id),
                "avatar_url": null,
                "cover_image_url": null,
                "location": null,
                "website": null,
                "birth_date": null,
                "phone_number": null,
                "social_links": {},
                "privacy_settings": {
                    "profile_visible": true,
                    "email_visible": false
                },
                "created_at": chrono::Utc::now().timestamp(),
                "updated_at": null
            });
            runner.database.put(profile_key.as_bytes(), profile_data.to_string().as_bytes())?;
            
            // Create user preferences
            let prefs_key = format!("user_preferences_{:03}", user_id);
            let prefs_data = serde_json::json!({
                "id": user_id,
                "user_id": user_id,
                "theme": "light",
                "language": "en",
                "currency": "USD",
                "email_notifications": {
                    "order_updates": true,
                    "promotions": false,
                    "newsletter": false,
                    "security_alerts": true
                },
                "push_notifications": {
                    "order_updates": true,
                    "promotions": false,
                    "recommendations": true
                },
                "accessibility_settings": null,
                "created_at": chrono::Utc::now().timestamp(),
                "updated_at": null
            });
            runner.database.put(prefs_key.as_bytes(), prefs_data.to_string().as_bytes())?;
            
            migrated_count += 1;
        }
    }
    
    println!("✅ Migrated {} users with profiles and preferences", migrated_count);
    Ok(())
}

/// Validate the user profiles migration
fn validate_user_profiles_migration(database: &Database) -> Result<()> {
    println!("🔍 Validating user profiles migration...");
    
    // Check counts
    let users_count = validation::count_records_with_prefix(database, "users_")?;
    let profiles_count = validation::count_records_with_prefix(database, "user_profiles_")?;
    let prefs_count = validation::count_records_with_prefix(database, "user_preferences_")?;
    
    println!("📊 Migration counts: {} users, {} profiles, {} preferences", 
             users_count, profiles_count, prefs_count);
    
    // Validate JSON structure for profiles
    if let Some(profile_data) = database.get(b"user_profiles_001")? {
        validation::validate_json_structure(&profile_data, &["id", "user_id", "display_name"])?;
    }
    
    // Validate JSON structure for preferences
    if let Some(prefs_data) = database.get(b"user_preferences_001")? {
        validation::validate_json_structure(&prefs_data, &["id", "user_id", "theme", "language"])?;
    }
    
    println!("✅ User profiles migration validation successful");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_user_profiles_migration() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let migration = AddUserProfilesMigration {};
        
        // Test migration steps
        let steps = migration.steps();
        assert!(steps.len() > 10); // Should have table creation + column additions + indexes
        
        // Test version
        assert_eq!(migration.version(), SchemaVersion::new(1, 1));
        
        // Test schema
        let schema = migration.target_schema();
        assert!(schema.tables.len() >= 8); // Original 5 + 4 new tables
        assert!(schema.constraints.len() >= 5); // Original 2 + 3 new constraints
    }

    #[test]
    fn test_user_profiles_table() {
        let table = create_user_profiles_table();
        assert_eq!(table.name, "user_profiles");
        assert_eq!(table.columns.len(), 14);
        assert_eq!(table.primary_key, vec!["id".to_string()]);
    }

    #[test]
    fn test_enhanced_users_table() {
        let table = create_enhanced_users_table();
        assert_eq!(table.name, "users");
        // Should have original columns + 3 new ones
        assert!(table.columns.len() >= 12);
        
        // Check for new columns
        let column_names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"last_login_at"));
        assert!(column_names.contains(&"email_verified"));
        assert!(column_names.contains(&"timezone"));
    }
}