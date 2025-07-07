use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Minimal Lightning DB Test");

    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path().join("test.db"), config)?;

    // Basic put/get test
    db.put(b"test_key", b"test_value")?;
    let result = db.get(b"test_key")?;

    if result == Some(b"test_value".to_vec()) {
        println!("✅ Basic put/get: PASS");
    } else {
        println!("❌ Basic put/get: FAIL");
        return Ok(());
    }

    // Delete test
    db.delete(b"test_key")?;
    let result = db.get(b"test_key")?;

    if result.is_none() {
        println!("✅ Delete: PASS");
    } else {
        println!("❌ Delete: FAIL");
        return Ok(());
    }

    // Transaction test
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"tx_key", b"tx_value")?;
    db.commit_transaction(tx_id)?;

    let result = db.get(b"tx_key")?;
    if result == Some(b"tx_value".to_vec()) {
        println!("✅ Transaction: PASS");
    } else {
        println!("❌ Transaction: FAIL");
        return Ok(());
    }

    println!("🎉 All tests passed!");
    Ok(())
}
