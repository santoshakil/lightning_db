use lightning_db::features::migration::*;
use lightning_db::features::migration::implementations::*;
use std::collections::HashMap;
use tempfile::tempdir;

#[test]
fn test_migration_version_ordering() {
    let v1 = MigrationVersion::new(1);
    let v2 = MigrationVersion::new(2);
    let v3 = MigrationVersion::new(3);
    
    assert!(v1 < v2);
    assert!(v2 < v3);
    assert!(v1 < v3);
    
    assert_eq!(v1.next(), v2);
    assert_eq!(v2.prev(), Some(v1));
    assert_eq!(MigrationVersion::INITIAL.prev(), None);
}

#[test]
fn test_migration_manager_creation() {
    let manager = MigrationManager::new();
    assert_eq!(manager.get_pending_migrations(MigrationVersion::INITIAL, None).len(), 0);
}

#[test]
fn test_migration_template_creation() {
    let template_manager = MigrationTemplateManager::new();
    
    let templates = template_manager.list_templates();
    assert!(templates.contains(&"create_table"));
    assert!(templates.contains(&"add_column"));
    assert!(templates.contains(&"create_index"));
    assert!(templates.contains(&"data_migration"));
    
    let mut params = HashMap::new();
    params.insert("table_name".to_string(), "users".to_string());
    params.insert("columns".to_string(), "name VARCHAR(255), email VARCHAR(255)".to_string());
    
    let migration = template_manager.generate_migration(
        "create_table",
        MigrationVersion::new(1),
        "create_users_table",
        &params,
    );
    
    assert!(migration.is_ok());
    let migration = migration.unwrap();
    assert_eq!(migration.metadata.version, MigrationVersion::new(1));
    assert_eq!(migration.metadata.name, "create_users_table");
    assert!(migration.up_script.contains("CREATE TABLE users"));
    assert!(migration.down_script.is_some());
    assert!(migration.down_script.unwrap().contains("DROP TABLE IF EXISTS users"));
}

#[test]
fn test_schema_manager() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    
    let schema_manager = SchemaManager::new();
    
    let result = schema_manager.initialize_schema(db_path);
    assert!(result.is_ok());
    
    let version = schema_manager.get_current_version(db_path);
    assert!(version.is_ok());
    assert_eq!(version.unwrap(), MigrationVersion::INITIAL);
}

#[test]
fn test_migration_context() {
    let ctx = MigrationContext::new(
        "./test_db".to_string(),
        MigrationVersion::INITIAL,
        MigrationVersion::new(5),
    )
    .with_dry_run(true)
    .with_batch_size(500);
    
    assert_eq!(ctx.current_version, MigrationVersion::INITIAL);
    assert_eq!(ctx.target_version, MigrationVersion::new(5));
    assert!(ctx.dry_run);
    assert_eq!(ctx.batch_size, 500);
}

#[test]
fn test_migration_validator() {
    let validator = MigrationValidator::new();
    
    let migration = Migration {
        metadata: MigrationMetadata {
            version: MigrationVersion::new(1),
            name: "test_migration".to_string(),
            description: "Test migration".to_string(),
            migration_type: MigrationType::Schema,
            mode: MigrationMode::Offline,
            author: "test".to_string(),
            created_at: std::time::SystemTime::now(),
            dependencies: vec![],
            reversible: true,
            estimated_duration: None,
            checksum: calculate_checksum("CREATE TABLE test (id INTEGER);"),
        },
        up_script: "CREATE TABLE test (id INTEGER);".to_string(),
        down_script: Some("DROP TABLE test;".to_string()),
        validation_script: None,
    };
    
    let ctx = MigrationContext::new(
        "./test_db".to_string(),
        MigrationVersion::INITIAL,
        MigrationVersion::new(1),
    );
    
    let result = validator.validate_migration(&migration, &ctx);
    assert!(result.is_ok());
    
    let validation_result = result.unwrap();
    assert!(validation_result.is_valid);
}

#[test]
fn test_progress_tracker() {
    let progress = std::sync::Arc::new(parking_lot::RwLock::new(MigrationProgress::new()));
    let tracker = ProgressTracker::new(progress.clone());
    
    tracker.start_migration_batch(3);
    
    let snapshot = tracker.get_progress_snapshot();
    assert_eq!(snapshot.total_migrations, 3);
    assert_eq!(snapshot.completed_migrations, 0);
    assert_eq!(snapshot.progress_percentage, 0);
}

#[test]
fn test_rollback_plan_creation() {
    let rollback_manager = RollbackManager::new();
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    
    let plan = rollback_manager.create_rollback_plan(
        db_path,
        MigrationVersion::new(1),
        RollbackStrategy::Graceful,
        &HashMap::new(),
    );
    
    // This will error because we don't have actual migration history, but it tests the API
    assert!(plan.is_err()); // Expected to fail without actual data
}

#[test]
fn test_data_transformation_engine() {
    let mut engine = DataTransformationEngine::new()
        .with_batch_size(100)
        .with_parallel_workers(2);
    
    engine.add_transformation(TransformationType::AddColumn {
        name: "created_at".to_string(),
        data_type: "TIMESTAMP".to_string(),
        default_value: Some("CURRENT_TIMESTAMP".to_string()),
    });
    
    // Test validation (dry run)
    let ctx = MigrationContext::new(
        "./test_db".to_string(),
        MigrationVersion::INITIAL,
        MigrationVersion::new(1),
    ).with_dry_run(true);
    
    let mut ctx_mut = ctx;
    let result = engine.execute_transformations(&mut ctx_mut);
    assert!(result.is_ok());
}

#[test]
fn test_migration_generator() {
    let version = MigrationGenerator::generate_timestamp_version();
    assert!(version.as_u64() > 0);
    
    let v1 = MigrationVersion::new(5);
    let v2 = MigrationGenerator::generate_sequential_version(v1);
    assert_eq!(v2, MigrationVersion::new(6));
    
    let filename = MigrationGenerator::generate_migration_filename(
        MigrationVersion::new(1),
        "Create Users Table"
    );
    assert_eq!(filename, "0001_create_users_table.sql");
}