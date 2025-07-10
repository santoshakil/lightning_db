use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Create database without LSM tree to force B+Tree usage
    let config = LightningDbConfig {
        compression_enabled: false,
        ..Default::default()
    }; // This disables LSM tree

    let db = Database::create(&db_path, config.clone())?;

    // Write directly
    println!("Writing key_direct...");
    db.put(b"key_direct", b"value_direct")?;

    // Read back
    println!("Reading key_direct...");
    match db.get(b"key_direct")? {
        Some(val) => println!("Found: {:?}", String::from_utf8_lossy(&val)),
        None => println!("Not found!"),
    }

    // Drop and reopen
    drop(db);

    println!("\nReopening database...");
    let db2 = Database::open(&db_path, config)?;

    println!("Reading key_direct from reopened db...");
    match db2.get(b"key_direct")? {
        Some(val) => println!("Found: {:?}", String::from_utf8_lossy(&val)),
        None => println!("Not found!"),
    }

    Ok(())
}
