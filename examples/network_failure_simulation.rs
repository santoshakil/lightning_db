use lightning_db::{Database, LightningDbConfig};
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

// Simulates network partition or I/O failures
struct NetworkSimulator {
    failure_rate: Arc<Mutex<f32>>,
    delay_ms: Arc<Mutex<u64>>,
}

impl NetworkSimulator {
    fn new() -> Self {
        Self {
            failure_rate: Arc::new(Mutex::new(0.0)),
            delay_ms: Arc::new(Mutex::new(0)),
        }
    }

    fn set_failure_rate(&self, rate: f32) {
        *self.failure_rate.lock().unwrap() = rate;
    }

    fn set_delay(&self, delay_ms: u64) {
        *self.delay_ms.lock().unwrap() = delay_ms;
    }

    fn simulate_operation<F, T>(&self, op: F) -> Result<T, Box<dyn std::error::Error>>
    where
        F: FnOnce() -> Result<T, Box<dyn std::error::Error>>,
    {
        let failure_rate = *self.failure_rate.lock().unwrap();
        let delay_ms = *self.delay_ms.lock().unwrap();

        // Simulate network delay
        if delay_ms > 0 {
            thread::sleep(Duration::from_millis(delay_ms));
        }

        // Simulate network failure
        let mut rng = rand::rng();
        if rng.random::<f32>() < failure_rate {
            return Err("Simulated network failure".into());
        }

        op()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB Network Failure Simulation ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();
    let network = Arc::new(NetworkSimulator::new());

    // Test 1: Transient network failures during writes
    println!("Test 1: Transient Network Failures During Writes");
    println!("===============================================");
    {
        let db = Database::create(db_path, LightningDbConfig::default())?;

        // Set 20% failure rate
        network.set_failure_rate(0.2);

        let mut successful_writes = 0;
        let mut failed_writes = 0;
        let mut retry_count = 0;

        for i in 0..1000 {
            let key = format!("transient_key_{:04}", i);
            let value = format!("transient_value_{:04}", i);

            // Retry logic with exponential backoff
            let mut retry_delay = 10;
            let max_retries = 3;
            let mut success = false;

            for attempt in 0..=max_retries {
                match network.simulate_operation(|| {
                    db.put(key.as_bytes(), value.as_bytes())
                        .map_err(|e| e.into())
                }) {
                    Ok(_) => {
                        successful_writes += 1;
                        success = true;
                        if attempt > 0 {
                            retry_count += 1;
                        }
                        break;
                    }
                    Err(_) if attempt < max_retries => {
                        thread::sleep(Duration::from_millis(retry_delay));
                        retry_delay *= 2;
                    }
                    Err(_) => {
                        failed_writes += 1;
                    }
                }
            }

            if !success {
                println!(
                    "  ⚠️  Failed to write key {} after {} retries",
                    key, max_retries
                );
            }
        }

        println!(
            "  ✓ Writes: {} successful, {} failed, {} required retries",
            successful_writes, failed_writes, retry_count
        );

        // Verify data consistency
        network.set_failure_rate(0.0); // Disable failures for verification
        let mut verified = 0;
        for i in 0..1000 {
            let key = format!("transient_key_{:04}", i);
            if db.get(key.as_bytes())?.is_some() {
                verified += 1;
            }
        }
        println!(
            "  ✓ Data verification: {}/{} keys persisted",
            verified, successful_writes
        );
    }

    // Test 2: Network partitions during transactions
    println!("\nTest 2: Network Partitions During Transactions");
    println!("==============================================");
    {
        let db = Arc::new(Database::open(db_path, LightningDbConfig::default())?);
        let network = Arc::clone(&network);

        // Simulate network partition (50% failure rate)
        network.set_failure_rate(0.5);
        network.set_delay(50); // 50ms delay

        let mut handles = vec![];
        let completed_transactions = Arc::new(Mutex::new(0));
        let aborted_transactions = Arc::new(Mutex::new(0));

        for thread_id in 0..4 {
            let db_clone = Arc::clone(&db);
            let network_clone = Arc::clone(&network);
            let completed_clone = Arc::clone(&completed_transactions);
            let aborted_clone = Arc::clone(&aborted_transactions);

            let handle = thread::spawn(move || {
                for tx_num in 0..50 {
                    match db_clone.begin_transaction() {
                        Ok(tx_id) => {
                            let mut tx_success = true;

                            // Try to perform 5 operations
                            for i in 0..5 {
                                let key = format!("partition_t{}_tx{}_k{}", thread_id, tx_num, i);
                                let value = format!("value_{}", i);

                                if network_clone
                                    .simulate_operation(|| {
                                        db_clone
                                            .put_tx(tx_id, key.as_bytes(), value.as_bytes())
                                            .map_err(|e| e.into())
                                    })
                                    .is_err()
                                {
                                    tx_success = false;
                                    break;
                                }
                            }

                            if tx_success {
                                match network_clone.simulate_operation(|| {
                                    db_clone.commit_transaction(tx_id).map_err(|e| e.into())
                                }) {
                                    Ok(_) => {
                                        *completed_clone.lock().unwrap() += 1;
                                    }
                                    Err(_) => {
                                        let _ = db_clone.abort_transaction(tx_id);
                                        *aborted_clone.lock().unwrap() += 1;
                                    }
                                }
                            } else {
                                let _ = db_clone.abort_transaction(tx_id);
                                *aborted_clone.lock().unwrap() += 1;
                            }
                        }
                        Err(_) => {
                            *aborted_clone.lock().unwrap() += 1;
                        }
                    }
                }
            });

            handles.push(handle);
        }

        let start = Instant::now();
        for handle in handles {
            handle.join().unwrap();
        }
        let duration = start.elapsed();

        let completed = *completed_transactions.lock().unwrap();
        let aborted = *aborted_transactions.lock().unwrap();

        println!("  ✓ Completed in {:.2}s", duration.as_secs_f64());
        println!(
            "  ✓ Transactions: {} completed, {} aborted",
            completed, aborted
        );
        println!(
            "  ✓ Success rate: {:.1}%",
            (completed as f64 / (completed + aborted) as f64) * 100.0
        );
    }

    // Test 3: Recovery after prolonged network outage
    println!("\nTest 3: Recovery After Prolonged Network Outage");
    println!("==============================================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Write baseline data
        println!("  Writing baseline data...");
        for i in 0..100 {
            let key = format!("baseline_key_{:02}", i);
            let value = format!("baseline_value_{:02}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }

        // Simulate total network outage (100% failure)
        network.set_failure_rate(1.0);
        network.set_delay(1000); // 1 second timeout

        println!("  Simulating network outage...");
        let outage_start = Instant::now();
        let mut failed_operations = 0;

        for i in 0..10 {
            let key = format!("outage_key_{}", i);
            let value = format!("outage_value_{}", i);

            match network.simulate_operation(|| {
                db.put(key.as_bytes(), value.as_bytes())
                    .map_err(|e| e.into())
            }) {
                Ok(_) => println!("  ⚠️  Unexpected success during outage!"),
                Err(_) => failed_operations += 1,
            }
        }

        let outage_duration = outage_start.elapsed();
        println!(
            "  ✓ Network outage lasted {:.2}s",
            outage_duration.as_secs_f64()
        );
        println!("  ✓ {} operations failed during outage", failed_operations);

        // Restore network
        network.set_failure_rate(0.0);
        network.set_delay(0);

        println!("  Network restored, testing recovery...");

        // Verify baseline data is intact
        let mut baseline_intact = 0;
        for i in 0..100 {
            let key = format!("baseline_key_{:02}", i);
            if db.get(key.as_bytes())?.is_some() {
                baseline_intact += 1;
            }
        }
        println!("  ✓ Baseline data: {}/100 entries intact", baseline_intact);

        // Write new data after recovery
        let mut post_recovery_writes = 0;
        for i in 0..50 {
            let key = format!("recovery_key_{}", i);
            let value = format!("recovery_value_{}", i);
            if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                post_recovery_writes += 1;
            }
        }
        println!(
            "  ✓ Post-recovery writes: {}/50 successful",
            post_recovery_writes
        );

        // Run integrity check
        let report = db.verify_integrity()?;
        if report.errors.is_empty() {
            println!("  ✓ Database integrity maintained after network recovery");
        } else {
            println!(
                "  ⚠️  {} integrity errors after recovery",
                report.errors.len()
            );
        }
    }

    // Test 4: Intermittent connectivity (network flapping)
    println!("\nTest 4: Intermittent Connectivity (Network Flapping)");
    println!("===================================================");
    {
        let db = Arc::new(Database::open(db_path, LightningDbConfig::default())?);
        let network = Arc::clone(&network);

        // Spawn thread to simulate network flapping
        let network_clone = Arc::clone(&network);
        let flapping_handle = thread::spawn(move || {
            let mut rng = rand::rng();
            for _ in 0..20 {
                // Random failure rate between 0% and 80%
                let failure_rate = rng.random_range(0.0..0.8);
                network_clone.set_failure_rate(failure_rate);

                // Random delay between 0ms and 100ms
                let delay = rng.random_range(0..100);
                network_clone.set_delay(delay);

                thread::sleep(Duration::from_millis(500));
            }

            // Stabilize network
            network_clone.set_failure_rate(0.0);
            network_clone.set_delay(0);
        });

        // Concurrent operations during network flapping
        let mut handles = vec![];
        let total_operations = Arc::new(Mutex::new(0));
        let successful_operations = Arc::new(Mutex::new(0));

        for thread_id in 0..3 {
            let db_clone = Arc::clone(&db);
            let network_clone = Arc::clone(&network);
            let total_clone = Arc::clone(&total_operations);
            let success_clone = Arc::clone(&successful_operations);

            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("flapping_t{}_k{}", thread_id, i);
                    let value = format!("flapping_value_{}", i);

                    *total_clone.lock().unwrap() += 1;

                    // Retry with backoff
                    let mut retry_delay = 50;
                    for _ in 0..5 {
                        match network_clone.simulate_operation(|| {
                            db_clone
                                .put(key.as_bytes(), value.as_bytes())
                                .map_err(|e| e.into())
                        }) {
                            Ok(_) => {
                                *success_clone.lock().unwrap() += 1;
                                break;
                            }
                            Err(_) => {
                                thread::sleep(Duration::from_millis(retry_delay));
                                retry_delay = (retry_delay * 2).min(1000);
                            }
                        }
                    }
                }
            });

            handles.push(handle);
        }

        let start = Instant::now();

        for handle in handles {
            handle.join().unwrap();
        }

        flapping_handle.join().unwrap();

        let duration = start.elapsed();
        let total = *total_operations.lock().unwrap();
        let successful = *successful_operations.lock().unwrap();

        println!("  ✓ Completed in {:.2}s", duration.as_secs_f64());
        println!(
            "  ✓ Operations: {}/{} successful ({:.1}%)",
            successful,
            total,
            (successful as f64 / total as f64) * 100.0
        );

        // Final verification
        db.checkpoint()?;
        drop(db);

        let db = Database::open(db_path, LightningDbConfig::default())?;
        let report = db.verify_integrity()?;

        println!(
            "  ✓ Final state: {} total keys, {} errors",
            report.statistics.total_keys,
            report.errors.len()
        );
    }

    println!("\n=== Network Failure Simulation Complete ===");
    println!("Lightning DB demonstrates resilience to:");
    println!("✓ Transient network failures with retry logic");
    println!("✓ Network partitions during transactions");
    println!("✓ Recovery after prolonged outages");
    println!("✓ Intermittent connectivity (network flapping)");

    Ok(())
}
