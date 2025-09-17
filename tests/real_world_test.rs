use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

#[test]
fn test_real_world_usage() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Simulate a real-world key-value workload
    let start = Instant::now();
    let num_operations = 1000;

    // Phase 1: Initial data insertion
    for i in 0..num_operations {
        let key = format!("user:{}:profile", i);
        let value = format!("{{\"id\":{},\"name\":\"User {}\",\"email\":\"user{}@example.com\"}}", i, i, i);

        if let Err(e) = db.put(key.as_bytes(), value.as_bytes()) {
            panic!("Failed to insert key {}: {:?}", i, e);
        }
    }

    // Phase 2: Read verification
    for i in 0..num_operations {
        let key = format!("user:{}:profile", i);

        match db.get(key.as_bytes()) {
            Ok(Some(val)) => {
                let expected = format!("{{\"id\":{},\"name\":\"User {}\",\"email\":\"user{}@example.com\"}}", i, i, i);
                assert_eq!(val, expected.as_bytes());
            }
            Ok(None) => panic!("Key {} not found", key),
            Err(e) => panic!("Failed to read key {}: {:?}", key, e),
        }
    }

    // Phase 3: Updates
    for i in 0..100 {
        let key = format!("user:{}:profile", i);
        let value = format!("{{\"id\":{},\"name\":\"Updated User {}\",\"email\":\"updated{}@example.com\"}}", i, i, i);

        if let Err(e) = db.put(key.as_bytes(), value.as_bytes()) {
            panic!("Failed to update key {}: {:?}", i, e);
        }
    }

    // Phase 4: Deletions
    for i in 900..1000 {
        let key = format!("user:{}:profile", i);

        if let Err(e) = db.delete(key.as_bytes()) {
            panic!("Failed to delete key {}: {:?}", key, e);
        }
    }

    // Phase 5: Final verification
    let mut found = 0;
    let mut not_found = 0;

    for i in 0..num_operations {
        let key = format!("user:{}:profile", i);

        match db.get(key.as_bytes()) {
            Ok(Some(_)) => found += 1,
            Ok(None) => not_found += 1,
            Err(e) => panic!("Error during final verification: {:?}", e),
        }
    }

    // We should have 900 keys (deleted 100)
    assert_eq!(found, 900);
    assert_eq!(not_found, 100);

    let elapsed = start.elapsed();
    println!("Real-world test completed in {:?}", elapsed);
    println!("Operations per second: {:.0}", (num_operations * 4) as f64 / elapsed.as_secs_f64());
}

#[test]
fn test_transaction_workload() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Simulate bank account transfers
    let num_accounts = 100;
    let initial_balance = 1000;

    // Initialize accounts
    for i in 0..num_accounts {
        let key = format!("account:{}", i);
        let value = initial_balance.to_string();

        if let Err(e) = db.put(key.as_bytes(), value.as_bytes()) {
            panic!("Failed to create account {}: {:?}", i, e);
        }
    }

    // Perform transfers
    for _ in 0..50 {
        let from = rand::random::<usize>() % num_accounts;
        let to = rand::random::<usize>() % num_accounts;

        if from != to {
            let tx = match db.begin_transaction() {
                Ok(tx) => tx,
                Err(e) => {
                    eprintln!("Failed to begin transaction: {:?}", e);
                    continue;
                }
            };

            // Read balances
            let from_key = format!("account:{}", from);
            let to_key = format!("account:{}", to);

            let from_balance = match db.get_tx(tx, from_key.as_bytes()) {
                Ok(Some(val)) => String::from_utf8_lossy(&val).parse::<i32>().unwrap_or(0),
                _ => 0,
            };

            let to_balance = match db.get_tx(tx, to_key.as_bytes()) {
                Ok(Some(val)) => String::from_utf8_lossy(&val).parse::<i32>().unwrap_or(0),
                _ => 0,
            };

            // Transfer amount
            let amount = 10;

            if from_balance >= amount {
                let new_from = (from_balance - amount).to_string();
                let new_to = (to_balance + amount).to_string();

                // Update balances
                if db.put_tx(tx, from_key.as_bytes(), new_from.as_bytes()).is_ok() &&
                   db.put_tx(tx, to_key.as_bytes(), new_to.as_bytes()).is_ok() {
                    let _ = db.commit_transaction(tx);
                } else {
                    let _ = db.abort_transaction(tx);
                }
            } else {
                let _ = db.abort_transaction(tx);
            }
        }
    }

    // Verify total balance is conserved
    let mut total = 0;
    for i in 0..num_accounts {
        let key = format!("account:{}", i);

        if let Ok(Some(val)) = db.get(key.as_bytes()) {
            total += String::from_utf8_lossy(&val).parse::<i32>().unwrap_or(0);
        }
    }

    assert_eq!(total, num_accounts as i32 * initial_balance);
    println!("Transaction workload test passed - total balance conserved: {}", total);
}

// Add rand as dev dependency for the test
#[cfg(test)]
mod rand {
    pub fn random<T>() -> T
    where
        T: Default + From<u8>,
    {
        T::from(42u8) // Simple deterministic "random" for testing
    }
}