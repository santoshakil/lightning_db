/// Debug Transaction Issue
///
/// Investigate why all transactions are failing in the critical test
use lightning_db::{Database, LightningDbConfig};
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 DEBUGGING TRANSACTION ISSUE");
    println!("===============================");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    println!("✅ Database created successfully");

    // Test 1: Basic transaction operations
    println!("\n1️⃣ Testing basic transaction operations:");

    match db.begin_transaction() {
        Ok(tx_id) => {
            println!("   ✅ Transaction {} started successfully", tx_id);

            // Test transaction put
            match db.put_tx(tx_id, b"test_key", b"test_value") {
                Ok(_) => println!("   ✅ Transaction put successful"),
                Err(e) => println!("   ❌ Transaction put failed: {}", e),
            }

            // Test transaction get
            match db.get_tx(tx_id, b"test_key") {
                Ok(Some(value)) => println!(
                    "   ✅ Transaction get successful: {:?}",
                    String::from_utf8_lossy(&value)
                ),
                Ok(None) => println!("   ⚠️ Transaction get returned None"),
                Err(e) => println!("   ❌ Transaction get failed: {}", e),
            }

            // Test commit
            match db.commit_transaction(tx_id) {
                Ok(_) => println!("   ✅ Transaction commit successful"),
                Err(e) => println!("   ❌ Transaction commit failed: {}", e),
            }
        }
        Err(e) => {
            println!("   ❌ Failed to start transaction: {}", e);
            return Ok(());
        }
    }

    // Test 2: Bank transfer simulation (single transfer)
    println!("\n2️⃣ Testing single bank transfer:");

    // Initialize accounts
    db.put(b"account_0", b"1000")?;
    db.put(b"account_1", b"1000")?;
    println!("   ✅ Accounts initialized");

    // Test single transfer
    match test_single_transfer(&db, 0, 1, 100) {
        Ok(_) => println!("   ✅ Single transfer successful"),
        Err(e) => println!("   ❌ Single transfer failed: {}", e),
    }

    // Verify balances
    let balance_0 = get_account_balance(&db, 0)?;
    let balance_1 = get_account_balance(&db, 1)?;
    println!("   Account 0 balance: {} (expected: 900)", balance_0);
    println!("   Account 1 balance: {} (expected: 1100)", balance_1);

    // Test 3: Transfer with insufficient funds
    println!("\n3️⃣ Testing transfer with insufficient funds:");
    match test_single_transfer(&db, 0, 1, 2000) {
        Ok(_) => println!("   ⚠️ Transfer should have failed but succeeded"),
        Err(e) => println!("   ✅ Transfer correctly failed: {}", e),
    }

    // Test 4: Test transaction methods availability
    println!("\n4️⃣ Testing transaction method availability:");
    let tx_id = db.begin_transaction()?;

    // Check if all transaction methods exist
    println!("   Testing get_tx...");
    let _ = db.get_tx(tx_id, b"dummy");

    println!("   Testing put_tx...");
    let _ = db.put_tx(tx_id, b"dummy", b"dummy");

    println!("   Testing abort_transaction...");
    let _ = db.abort_transaction(tx_id);

    println!("   ✅ All transaction methods available");

    // Test 5: Check if there are any transaction configuration issues
    println!("\n5️⃣ Testing transaction configuration:");

    let stats = db.stats();
    println!("   Active transactions: {}", stats.active_transactions);

    // Try multiple transactions
    let tx1 = db.begin_transaction()?;
    let tx2 = db.begin_transaction()?;
    let tx3 = db.begin_transaction()?;

    println!(
        "   ✅ Created multiple transactions: {}, {}, {}",
        tx1, tx2, tx3
    );

    // Clean up
    let _ = db.abort_transaction(tx1);
    let _ = db.abort_transaction(tx2);
    let _ = db.abort_transaction(tx3);

    println!("\n📊 DIAGNOSIS COMPLETE");
    println!("If all tests above pass, the transaction system works correctly.");
    println!("The issue might be in the error handling or test logic.");

    Ok(())
}

fn test_single_transfer(
    db: &Database,
    from: usize,
    to: usize,
    amount: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let tx_id = db.begin_transaction()?;

    let from_key = format!("account_{}", from);
    let to_key = format!("account_{}", to);

    // Get current balances
    let from_balance: i32 = match db.get_tx(tx_id, from_key.as_bytes())? {
        Some(balance_bytes) => {
            let balance_str = std::str::from_utf8(&balance_bytes)?;
            balance_str.parse()?
        }
        None => {
            db.abort_transaction(tx_id)?;
            return Err("From account not found".into());
        }
    };

    let to_balance: i32 = match db.get_tx(tx_id, to_key.as_bytes())? {
        Some(balance_bytes) => {
            let balance_str = std::str::from_utf8(&balance_bytes)?;
            balance_str.parse()?
        }
        None => {
            db.abort_transaction(tx_id)?;
            return Err("To account not found".into());
        }
    };

    println!(
        "     From balance: {}, To balance: {}, Transfer amount: {}",
        from_balance, to_balance, amount
    );

    // Check sufficient funds
    if from_balance < amount {
        db.abort_transaction(tx_id)?;
        return Err("Insufficient funds".into());
    }

    // Perform transfer
    let new_from_balance = from_balance - amount;
    let new_to_balance = to_balance + amount;

    db.put_tx(
        tx_id,
        from_key.as_bytes(),
        new_from_balance.to_string().as_bytes(),
    )?;
    db.put_tx(
        tx_id,
        to_key.as_bytes(),
        new_to_balance.to_string().as_bytes(),
    )?;

    db.commit_transaction(tx_id)?;
    Ok(())
}

fn get_account_balance(db: &Database, account: usize) -> Result<i32, Box<dyn std::error::Error>> {
    let key = format!("account_{}", account);
    match db.get(key.as_bytes())? {
        Some(balance_bytes) => {
            let balance_str = std::str::from_utf8(&balance_bytes)?;
            Ok(balance_str.parse()?)
        }
        None => Err("Account not found".into()),
    }
}
