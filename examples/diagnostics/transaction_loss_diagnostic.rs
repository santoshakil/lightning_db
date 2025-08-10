use lightning_db::{Database, LightningDbConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::thread;
use tempfile::tempdir;

#[derive(Debug, Clone)]
struct TransactionEvent {
    tx_id: u64,
    thread_id: usize,
    event_type: EventType,
    timestamp: std::time::Instant,
    counter_value: Option<u64>,
}

#[derive(Debug, Clone)]
enum EventType {
    Begin,
    Read(u64),
    Write(u64),
    CommitAttempt,
    CommitSuccess,
    CommitFailed(String),
    Abort,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Loss Diagnostic");
    println!("=========================================\n");

    // Test with regular transaction manager
    println!("Testing Regular Transaction Manager");
    println!("------------------------------------");
    let (events, lost_updates) = test_with_detailed_logging(false)?;
    analyze_lost_transactions(events, lost_updates);

    Ok(())
}

fn test_with_detailed_logging(use_optimized: bool) -> Result<(Vec<TransactionEvent>, Vec<u64>), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.use_optimized_transactions = use_optimized;
    let db = Arc::new(Database::create(dir.path(), config)?);

    // Initialize counter
    db.put(b"counter", b"0")?;
    
    let events = Arc::new(Mutex::new(Vec::new()));
    let successful_values = Arc::new(Mutex::new(Vec::new()));
    let start_time = std::time::Instant::now();
    
    // Launch 10 threads, each doing 10 increments (for easier analysis)
    let mut handles = vec![];
    for thread_id in 0..10 {
        let db = db.clone();
        let events = events.clone();
        let successful_values = successful_values.clone();
        
        let handle = thread::spawn(move || {
            for iteration in 0..10 {
                // Begin transaction
                let tx_id = match db.begin_transaction() {
                    Ok(id) => {
                        events.lock().unwrap().push(TransactionEvent {
                            tx_id: id,
                            thread_id,
                            event_type: EventType::Begin,
                            timestamp: std::time::Instant::now(),
                            counter_value: None,
                        });
                        id
                    },
                    Err(e) => {
                        println!("Thread {} iter {}: Failed to begin transaction: {}", thread_id, iteration, e);
                        continue;
                    }
                };
                
                // Read current value
                let current = match db.get_tx(tx_id, b"counter") {
                    Ok(Some(v)) => {
                        let val = String::from_utf8_lossy(&v).parse::<u64>().unwrap_or(0);
                        events.lock().unwrap().push(TransactionEvent {
                            tx_id,
                            thread_id,
                            event_type: EventType::Read(val),
                            timestamp: std::time::Instant::now(),
                            counter_value: Some(val),
                        });
                        val
                    },
                    Ok(None) => {
                        events.lock().unwrap().push(TransactionEvent {
                            tx_id,
                            thread_id,
                            event_type: EventType::Read(0),
                            timestamp: std::time::Instant::now(),
                            counter_value: Some(0),
                        });
                        0
                    },
                    Err(e) => {
                        println!("Thread {} iter {}: Read failed: {}", thread_id, iteration, e);
                        let _ = db.abort_transaction(tx_id);
                        events.lock().unwrap().push(TransactionEvent {
                            tx_id,
                            thread_id,
                            event_type: EventType::Abort,
                            timestamp: std::time::Instant::now(),
                            counter_value: None,
                        });
                        continue;
                    }
                };
                
                // Increment
                let new_value = current + 1;
                
                // Write new value
                if let Err(e) = db.put_tx(tx_id, b"counter", new_value.to_string().as_bytes()) {
                    println!("Thread {} iter {}: Write failed: {}", thread_id, iteration, e);
                    let _ = db.abort_transaction(tx_id);
                    events.lock().unwrap().push(TransactionEvent {
                        tx_id,
                        thread_id,
                        event_type: EventType::Abort,
                        timestamp: std::time::Instant::now(),
                        counter_value: None,
                    });
                    continue;
                }
                
                events.lock().unwrap().push(TransactionEvent {
                    tx_id,
                    thread_id,
                    event_type: EventType::Write(new_value),
                    timestamp: std::time::Instant::now(),
                    counter_value: Some(new_value),
                });
                
                // Try to commit
                events.lock().unwrap().push(TransactionEvent {
                    tx_id,
                    thread_id,
                    event_type: EventType::CommitAttempt,
                    timestamp: std::time::Instant::now(),
                    counter_value: Some(new_value),
                });
                
                match db.commit_transaction(tx_id) {
                    Ok(_) => {
                        events.lock().unwrap().push(TransactionEvent {
                            tx_id,
                            thread_id,
                            event_type: EventType::CommitSuccess,
                            timestamp: std::time::Instant::now(),
                            counter_value: Some(new_value),
                        });
                        successful_values.lock().unwrap().push(new_value);
                    }
                    Err(e) => {
                        events.lock().unwrap().push(TransactionEvent {
                            tx_id,
                            thread_id,
                            event_type: EventType::CommitFailed(e.to_string()),
                            timestamp: std::time::Instant::now(),
                            counter_value: Some(new_value),
                        });
                        let _ = db.abort_transaction(tx_id);
                    }
                }
                
                // Small delay to reduce contention slightly
                thread::sleep(std::time::Duration::from_micros(thread_id as u64 * 10));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        let _ = handle.join();
    }
    
    // Small delay to ensure everything is flushed
    thread::sleep(std::time::Duration::from_millis(100));
    
    // Check final value
    let final_value = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    
    let successful = successful_values.lock().unwrap();
    let expected = successful.len() as u64;
    
    println!("\nResults:");
    println!("  Total attempts: 100");
    println!("  Successful commits: {}", expected);
    println!("  Final counter value: {}", final_value);
    
    // Find lost updates
    let mut lost_updates = Vec::new();
    if final_value < expected {
        // Sort successful values
        let mut sorted_values = successful.clone();
        sorted_values.sort();
        
        // Find which values are missing from 1..final_value
        for i in 1..=expected {
            if i > final_value {
                lost_updates.push(sorted_values[(i-1) as usize]);
            }
        }
        
        println!("  ❌ Lost {} updates", lost_updates.len());
    } else if final_value == expected {
        println!("  ✅ All commits persisted correctly");
    }
    
    let events_clone = events.lock().unwrap().clone();
    Ok((events_clone, lost_updates))
}

fn analyze_lost_transactions(events: Vec<TransactionEvent>, lost_values: Vec<u64>) {
    if lost_values.is_empty() {
        println!("\n✅ No transactions were lost!");
        return;
    }
    
    println!("\n🔍 Analyzing Lost Transactions");
    println!("================================");
    
    // Group events by transaction ID
    let mut tx_events_map: HashMap<u64, Vec<TransactionEvent>> = HashMap::new();
    for event in &events {
        tx_events_map.entry(event.tx_id).or_insert(Vec::new()).push(event.clone());
    }
    
    // Find transactions that wrote the lost values
    for lost_value in &lost_values {
        println!("\n❌ Lost value: {}", lost_value);
        
        // Find the transaction that wrote this value
        for (tx_id, tx_events) in &tx_events_map {
            let mut wrote_value = false;
            let mut committed = false;
            let mut thread_id = 0;
            
            for event in tx_events.iter() {
                thread_id = event.thread_id;
                match &event.event_type {
                    EventType::Write(val) if *val == *lost_value => {
                        wrote_value = true;
                    }
                    EventType::CommitSuccess => {
                        committed = true;
                    }
                    _ => {}
                }
            }
            
            if wrote_value && committed {
                println!("  Transaction {} (Thread {}) claimed to commit value {}", 
                        tx_id, thread_id, lost_value);
                
                // Show the timeline for this transaction
                println!("  Timeline:");
                for event in tx_events.iter() {
                    let elapsed = event.timestamp.duration_since(events[0].timestamp);
                    match &event.event_type {
                        EventType::Begin => {
                            println!("    {:?}: BEGIN", elapsed);
                        }
                        EventType::Read(val) => {
                            println!("    {:?}: READ value={}", elapsed, val);
                        }
                        EventType::Write(val) => {
                            println!("    {:?}: WRITE value={}", elapsed, val);
                        }
                        EventType::CommitAttempt => {
                            println!("    {:?}: COMMIT ATTEMPT", elapsed);
                        }
                        EventType::CommitSuccess => {
                            println!("    {:?}: COMMIT SUCCESS ✓", elapsed);
                        }
                        EventType::CommitFailed(err) => {
                            println!("    {:?}: COMMIT FAILED: {}", elapsed, err);
                        }
                        EventType::Abort => {
                            println!("    {:?}: ABORT", elapsed);
                        }
                    }
                }
                
                // Check for concurrent transactions
                println!("  Concurrent transactions at commit time:");
                let commit_time = tx_events.iter()
                    .find(|e| matches!(e.event_type, EventType::CommitSuccess))
                    .map(|e| e.timestamp);
                
                if let Some(commit_time) = commit_time {
                    for (other_tx_id, other_events) in &tx_events_map {
                        if other_tx_id == tx_id {
                            continue;
                        }
                        
                        // Check if this transaction was active during our commit
                        let other_begin = other_events.iter()
                            .find(|e| matches!(e.event_type, EventType::Begin))
                            .map(|e| e.timestamp);
                        let other_end = other_events.iter()
                            .find(|e| matches!(e.event_type, EventType::CommitSuccess | EventType::Abort))
                            .map(|e| e.timestamp);
                        
                        if let (Some(begin), Some(end)) = (other_begin, other_end) {
                            if begin <= commit_time && end >= commit_time {
                                let other_thread = other_events[0].thread_id;
                                let other_write = other_events.iter()
                                    .find_map(|e| match e.event_type {
                                        EventType::Write(val) => Some(val),
                                        _ => None
                                    });
                                
                                if let Some(write_val) = other_write {
                                    println!("    - Tx {} (Thread {}) was writing value {}", 
                                            other_tx_id, other_thread, write_val);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Summary statistics
    println!("\n📊 Summary Statistics");
    println!("=====================");
    let total_commits = events.iter()
        .filter(|e| matches!(e.event_type, EventType::CommitSuccess))
        .count();
    let total_failures = events.iter()
        .filter(|e| matches!(e.event_type, EventType::CommitFailed(_)))
        .count();
    
    println!("  Total successful commits: {}", total_commits);
    println!("  Total failed commits: {}", total_failures);
    println!("  Lost transactions: {} ({:.1}% of successful)", 
            lost_values.len(),
            lost_values.len() as f64 / total_commits as f64 * 100.0);
    
    // Analyze failure reasons
    let mut failure_reasons: HashMap<String, usize> = HashMap::new();
    for event in &events {
        if let EventType::CommitFailed(reason) = &event.event_type {
            *failure_reasons.entry(reason.clone()).or_insert(0) += 1;
        }
    }
    
    if !failure_reasons.is_empty() {
        println!("\n  Failure reasons:");
        for (reason, count) in failure_reasons {
            println!("    - {}: {} times", reason, count);
        }
    }
}