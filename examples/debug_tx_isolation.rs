/// Debug transaction isolation issue
use lightning_db::{Database, LightningDbConfig};
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 DEBUG TRANSACTION ISOLATION");
    println!("==============================\n");

    let temp_dir = TempDir::new()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;

    // Setup initial data
    println!("Setting up initial data...");
    db.put(b"shared_key", b"initial_value")?;

    // Start two transactions
    println!("\nStarting two transactions...");
    let tx1 = db.begin_transaction()?;
    println!("Transaction 1 ID: {}", tx1);

    let tx2 = db.begin_transaction()?;
    println!("Transaction 2 ID: {}", tx2);

    // Both read the same key
    println!("\nBoth transactions reading shared_key...");
    let val1 = db.get_tx(tx1, b"shared_key")?;
    println!(
        "Tx1 read: {:?}",
        val1.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    let val2 = db.get_tx(tx2, b"shared_key")?;
    println!(
        "Tx2 read: {:?}",
        val2.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    // Tx1 modifies the key
    println!("\nTx1 modifying shared_key...");
    db.put_tx(tx1, b"shared_key", b"tx1_modified")?;

    // Check what Tx2 sees
    println!("\nTx2 reading again after Tx1's write...");
    let val2_after = db.get_tx(tx2, b"shared_key")?;
    println!(
        "Tx2 sees: {:?}",
        val2_after.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    // Now Tx2 also tries to write
    println!("\nTx2 attempting to write to shared_key...");
    db.put_tx(tx2, b"shared_key", b"tx2_modified")?;

    // Try to commit Tx1
    println!("\nCommitting Tx1...");
    match db.commit_transaction(tx1) {
        Ok(_) => println!("✅ Tx1 committed successfully"),
        Err(e) => println!("❌ Tx1 commit failed: {:?}", e),
    }

    // Try to commit Tx2 - should fail
    println!("\nCommitting Tx2 (should fail due to conflict)...");
    match db.commit_transaction(tx2) {
        Ok(_) => println!("❌ Tx2 committed successfully (UNEXPECTED!)"),
        Err(e) => println!("✅ Tx2 failed as expected: {:?}", e),
    }

    // Check final value
    println!("\nFinal value check...");
    let final_val = db.get(b"shared_key")?;
    println!(
        "Final value: {:?}",
        final_val.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    Ok(())
}
