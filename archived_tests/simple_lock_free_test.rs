use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path(), config)?;
    
    db.put(b"key1", b"value1")?;
    let value = db.get(b"key1")?;
    assert_eq!(value, Some(b"value1".to_vec()));
    
    println!("Lock-free test passed!");
    Ok(())
}