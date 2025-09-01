use lightning_db::Database;
use rand::distributions::{Alphanumeric, Distribution, Uniform};
use rand::Rng;
use rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkloadType {
    SequentialRead,
    RandomRead,
    SequentialWrite,
    RandomWrite,
    Mixed { read_ratio: f64 },
    Transaction { ops_per_tx: usize },
    Concurrent,
    Scan { key_count: usize, scan_length: usize },
    Batch { batch_size: usize },
    CacheTest,
    ReadHeavy,
    WriteHeavy,
    OLTP,
    OLAP,
    Recovery,
    Stress,
}

pub struct WorkloadGenerator {
    rng: ChaCha8Rng,
}

impl WorkloadGenerator {
    pub fn new() -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(42), // Fixed seed for reproducible benchmarks
        }
    }

    pub fn run_workload(
        &self,
        workload_type: &WorkloadType,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        dataset_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        match workload_type {
            WorkloadType::SequentialRead => self.sequential_read_workload(db, thread_count, value_size, dataset_size, duration),
            WorkloadType::RandomRead => self.random_read_workload(db, thread_count, value_size, dataset_size, duration),
            WorkloadType::SequentialWrite => self.sequential_write_workload(db, thread_count, value_size, duration),
            WorkloadType::RandomWrite => self.random_write_workload(db, thread_count, value_size, duration),
            WorkloadType::Mixed { read_ratio } => self.mixed_workload(db, thread_count, value_size, dataset_size, *read_ratio, duration),
            WorkloadType::Transaction { ops_per_tx } => self.transaction_workload(db, thread_count, value_size, *ops_per_tx, duration),
            WorkloadType::Concurrent => self.concurrent_workload(db, thread_count, value_size, dataset_size, duration),
            WorkloadType::Scan { key_count, scan_length } => self.scan_workload(db, thread_count, value_size, *key_count, *scan_length, duration),
            WorkloadType::Batch { batch_size } => self.batch_workload(db, thread_count, value_size, *batch_size, duration),
            WorkloadType::CacheTest => self.cache_test_workload(db, thread_count, value_size, duration),
            WorkloadType::ReadHeavy => self.mixed_workload(db, thread_count, value_size, dataset_size, 0.9, duration),
            WorkloadType::WriteHeavy => self.mixed_workload(db, thread_count, value_size, dataset_size, 0.1, duration),
            WorkloadType::OLTP => self.oltp_workload(db, thread_count, value_size, duration),
            WorkloadType::OLAP => self.olap_workload(db, thread_count, value_size, dataset_size, duration),
            WorkloadType::Recovery => self.recovery_workload(db, thread_count, value_size, duration),
            WorkloadType::Stress => self.stress_workload(db, thread_count, value_size, duration),
        }
    }

    fn sequential_read_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        dataset_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Pre-populate data
        let value = vec![0u8; value_size];
        for i in 0..dataset_size {
            let key = format!("seq_key_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        db.checkpoint()?;

        let ops_counter = Arc::new(AtomicU64::new(0));
        let start_time = Instant::now();

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                let duration = duration;
                
                thread::spawn(move || {
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        let key_index = (thread_id + local_ops as usize) % dataset_size;
                        let key = format!("seq_key_{:08}", key_index);
                        
                        if let Ok(_result) = db.get(key.as_bytes()) {
                            local_ops += 1;
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn random_read_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        dataset_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Pre-populate data
        let value = vec![0u8; value_size];
        for i in 0..dataset_size {
            let key = format!("rand_key_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        db.checkpoint()?;

        let ops_counter = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                
                thread::spawn(move || {
                    let mut rng = ChaCha8Rng::seed_from_u64(thread_id as u64);
                    let uniform = Uniform::from(0..dataset_size);
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        let key_index = uniform.sample(&mut rng);
                        let key = format!("rand_key_{:08}", key_index);
                        
                        if let Ok(_result) = db.get(key.as_bytes()) {
                            local_ops += 1;
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn sequential_write_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let ops_counter = Arc::new(AtomicU64::new(0));
        let value = vec![0u8; value_size];

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                let value = value.clone();
                
                thread::spawn(move || {
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        let key = format!("seq_write_{}_{:08}", thread_id, local_ops);
                        
                        if let Ok(()) = db.put(key.as_bytes(), &value) {
                            local_ops += 1;
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn random_write_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let ops_counter = Arc::new(AtomicU64::new(0));
        let value = vec![0u8; value_size];

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                let value = value.clone();
                
                thread::spawn(move || {
                    let mut rng = ChaCha8Rng::seed_from_u64(thread_id as u64);
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        let random_suffix: String = rng
                            .sample_iter(&Alphanumeric)
                            .take(10)
                            .map(char::from)
                            .collect();
                        
                        let key = format!("rand_write_{}_{}", thread_id, random_suffix);
                        
                        if let Ok(()) = db.put(key.as_bytes(), &value) {
                            local_ops += 1;
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn mixed_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        dataset_size: usize,
        read_ratio: f64,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Pre-populate data
        let value = vec![0u8; value_size];
        for i in 0..dataset_size {
            let key = format!("mixed_key_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        db.checkpoint()?;

        let ops_counter = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                let value = value.clone();
                
                thread::spawn(move || {
                    let mut rng = ChaCha8Rng::seed_from_u64(thread_id as u64);
                    let uniform = Uniform::from(0..dataset_size);
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        let should_read = rng.random::<f64>() < read_ratio;
                        
                        if should_read {
                            let key_index = uniform.sample(&mut rng);
                            let key = format!("mixed_key_{:08}", key_index);
                            
                            if let Ok(_result) = db.get(key.as_bytes()) {
                                local_ops += 1;
                            }
                        } else {
                            let key = format!("mixed_new_{}_{:08}", thread_id, local_ops);
                            
                            if let Ok(()) = db.put(key.as_bytes(), &value) {
                                local_ops += 1;
                            }
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn transaction_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        ops_per_tx: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let ops_counter = Arc::new(AtomicU64::new(0));
        let value = vec![0u8; value_size];

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                let value = value.clone();
                
                thread::spawn(move || {
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        if let Ok(tx_id) = db.begin_transaction() {
                            let mut tx_ops = 0;
                            
                            for i in 0..ops_per_tx {
                                let key = format!("tx_{}_{}_{}", thread_id, local_ops, i);
                                
                                if db.put_tx(tx_id, key.as_bytes(), &value).is_ok() {
                                    tx_ops += 1;
                                }
                            }
                            
                            if db.commit_transaction(tx_id).is_ok() {
                                local_ops += tx_ops;
                            }
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn concurrent_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        dataset_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Use mixed workload for concurrent testing
        self.mixed_workload(db, thread_count, value_size, dataset_size, 0.7, duration)
    }

    fn scan_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        key_count: usize,
        scan_length: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Pre-populate data
        let value = vec![0u8; value_size];
        for i in 0..key_count {
            let key = format!("scan_key_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        db.checkpoint()?;

        let ops_counter = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                
                thread::spawn(move || {
                    let mut rng = ChaCha8Rng::seed_from_u64(thread_id as u64);
                    let uniform = Uniform::from(0..=(key_count.saturating_sub(scan_length)));
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        let start_idx = uniform.sample(&mut rng);
                        
                        // Simulate range scan by reading consecutive keys
                        for i in 0..scan_length {
                            if start_idx + i < key_count {
                                let key = format!("scan_key_{:08}", start_idx + i);
                                if let Ok(_result) = db.get(key.as_bytes()) {
                                    local_ops += 1;
                                }
                            }
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn batch_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        batch_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let ops_counter = Arc::new(AtomicU64::new(0));
        let value = vec![0u8; value_size];

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                let value = value.clone();
                
                thread::spawn(move || {
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    let mut batch_count = 0u64;
                    
                    while thread_start.elapsed() < duration {
                        // Simulate batch by using transactions
                        if let Ok(tx_id) = db.begin_transaction() {
                            let mut batch_ops = 0;
                            
                            for i in 0..batch_size {
                                let key = format!("batch_{}_{:08}_{}", thread_id, batch_count, i);
                                
                                if db.put_tx(tx_id, key.as_bytes(), &value).is_ok() {
                                    batch_ops += 1;
                                }
                            }
                            
                            if db.commit_transaction(tx_id).is_ok() {
                                local_ops += batch_ops;
                                batch_count += 1;
                            }
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn cache_test_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Create a small hot dataset that should fit entirely in cache
        let hot_keys = 1000;
        let value = vec![0u8; value_size];
        
        for i in 0..hot_keys {
            let key = format!("hot_key_{:04}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        // Prime the cache
        for i in 0..hot_keys {
            let key = format!("hot_key_{:04}", i);
            let _ = db.get(key.as_bytes());
        }

        let ops_counter = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                
                thread::spawn(move || {
                    let mut rng = ChaCha8Rng::seed_from_u64(thread_id as u64);
                    let uniform = Uniform::from(0..hot_keys);
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        let key_index = uniform.sample(&mut rng);
                        let key = format!("hot_key_{:04}", key_index);
                        
                        if let Ok(_result) = db.get(key.as_bytes()) {
                            local_ops += 1;
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }

    fn oltp_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // OLTP: High frequency, small transactions, point lookups
        self.transaction_workload(db, thread_count, value_size, 3, duration)
    }

    fn olap_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        dataset_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // OLAP: Analytical workload with range scans
        self.scan_workload(db, thread_count, value_size, dataset_size, 1000, duration)
    }

    fn recovery_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Write data and measure recovery performance
        let ops = self.sequential_write_workload(db.clone(), thread_count, value_size, duration)?;
        
        // Force checkpoint to test persistence
        db.checkpoint().unwrap_or(());
        
        Ok(ops)
    }

    fn stress_workload(
        &self,
        db: Arc<Database>,
        thread_count: usize,
        value_size: usize,
        duration: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // High contention stress test
        let ops_counter = Arc::new(AtomicU64::new(0));
        let value = vec![0u8; value_size];
        let contention_keys = 10; // Small number of keys for high contention

        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                let ops_counter = Arc::clone(&ops_counter);
                let value = value.clone();
                
                thread::spawn(move || {
                    let mut rng = ChaCha8Rng::seed_from_u64(thread_id as u64);
                    let uniform = Uniform::from(0..contention_keys);
                    let mut local_ops = 0u64;
                    let thread_start = Instant::now();
                    
                    while thread_start.elapsed() < duration {
                        let should_read = rng.random::<f64>() < 0.5;
                        let key_index = uniform.sample(&mut rng);
                        let key = format!("stress_key_{:04}", key_index);
                        
                        if should_read {
                            if let Ok(_result) = db.get(key.as_bytes()) {
                                local_ops += 1;
                            }
                        } else {
                            if let Ok(()) = db.put(key.as_bytes(), &value) {
                                local_ops += 1;
                            }
                        }
                    }
                    
                    ops_counter.fetch_add(local_ops, Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().map_err(|_| "Thread join error")?;
        }

        Ok(ops_counter.load(Ordering::Relaxed))
    }
}

impl Default for WorkloadGenerator {
    fn default() -> Self {
        Self::new()
    }
}