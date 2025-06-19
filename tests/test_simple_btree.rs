use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

#[test]
fn test_simple_standard() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("simple_db");
    
    let config = LightningDbConfig {
        use_optimized_page_manager: false,
        prefetch_enabled: false,
        use_optimized_transactions: false,
        ..Default::default()
    };
    
    println!("Creating database...");
    let db = match Database::create(&db_path, config) {
        Ok(db) => {
            println!("Database created successfully!");
            db
        }
        Err(e) => {
            eprintln!("Failed to create database: {:?}", e);
            panic!("Database creation failed");
        }
    };
    
    println!("Writing key...");
    db.put(b"key1", b"value1").unwrap();
    println!("Key written successfully!");
    
    println!("Reading key...");
    let value = db.get(b"key1").unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
    println!("Key read successfully!");
}