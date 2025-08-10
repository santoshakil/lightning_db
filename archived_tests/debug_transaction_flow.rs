use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Flow Debug");
    println!("====================================\n");

    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.use_optimized_transactions = false; // Use regular manager
    let db = Arc::new(Database::create(dir.path(), config)?);

    // Initialize counter
    db.put(b"counter", b"0")?;
    println!("Initial value set to 0");

    // Start transaction 1
    let tx1 = db.begin_transaction()?;
    println!("\n[TX1] Started transaction {}", tx1);

    // Read current value
    let value1 = db.get_tx(tx1, b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("[TX1] Read value: {}", value1);

    // Write new value
    let new_value1 = value1 + 1;
    db.put_tx(tx1, b"counter", new_value1.to_string().as_bytes())?;
    println!("[TX1] Writing value: {}", new_value1);

    // Commit transaction 1
    db.commit_transaction(tx1)?;
    println!("[TX1] Committed successfully");

    // Read after commit
    let after_commit1 = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("[CHECK] Value after TX1 commit: {}", after_commit1);

    // Start transaction 2
    let tx2 = db.begin_transaction()?;
    println!("\n[TX2] Started transaction {}", tx2);

    // Read current value
    let value2 = db.get_tx(tx2, b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("[TX2] Read value: {}", value2);

    // Write new value
    let new_value2 = value2 + 1;
    db.put_tx(tx2, b"counter", new_value2.to_string().as_bytes())?;
    println!("[TX2] Writing value: {}", new_value2);

    // Commit transaction 2
    db.commit_transaction(tx2)?;
    println!("[TX2] Committed successfully");

    // Read after commit
    let after_commit2 = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("[CHECK] Value after TX2 commit: {}", after_commit2);

    // Force sync
    db.sync()?;
    println!("\n[SYNC] Database synced");

    // Read after sync
    let after_sync = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("[CHECK] Value after sync: {}", after_sync);

    // Close and reopen database
    drop(db);
    println!("\n[RESTART] Database closed");

    let db2 = Database::open(dir.path(), LightningDbConfig::default())?;
    let after_restart = db2.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("[CHECK] Value after restart: {}", after_restart);

    // Verify final state
    println!("\n=== VERIFICATION ===");
    println!("Expected final value: 2");
    println!("Actual final value: {}", after_restart);
    if after_restart == 2 {
        println!("✅ SUCCESS: All transactions persisted correctly");
    } else {
        println!("❌ FAILURE: Lost {} transactions", 2 - after_restart);
    }

    Ok(())
}