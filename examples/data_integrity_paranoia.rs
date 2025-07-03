use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use tempfile::TempDir;
use sha2::{Sha256, Digest};
use rand::{Rng, SeedableRng};

/// Custom integrity report for paranoia checks
#[derive(Debug)]
struct ParanoiaIntegrityReport {
    checked_items: usize,
    errors: Vec<String>,
    warnings: Vec<String>,
    corrupted_keys: usize,
    duration: Duration,
}

/// Data integrity paranoia test
/// Implements multiple layers of data validation and verification
#[derive(Debug, Clone)]
struct DataRecord {
    key: Vec<u8>,
    value: Vec<u8>,
    checksum: Vec<u8>,
    timestamp: u64,
    version: u64,
}

impl DataRecord {
    fn new(key: Vec<u8>, value: Vec<u8>, version: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        
        // Calculate checksum
        let mut hasher = Sha256::new();
        hasher.update(&key);
        hasher.update(&value);
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&version.to_le_bytes());
        let checksum = hasher.finalize().to_vec();
        
        Self {
            key,
            value,
            checksum,
            timestamp,
            version,
        }
    }
    
    fn verify(&self) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(&self.key);
        hasher.update(&self.value);
        hasher.update(&self.timestamp.to_le_bytes());
        hasher.update(&self.version.to_le_bytes());
        let calculated = hasher.finalize().to_vec();
        
        calculated == self.checksum
    }
    
    fn to_stored_value(&self) -> Vec<u8> {
        let mut result = Vec::new();
        
        // Store: [value_len][value][checksum][timestamp][version]
        result.extend(&(self.value.len() as u32).to_le_bytes());
        result.extend(&self.value);
        result.extend(&self.checksum);
        result.extend(&self.timestamp.to_le_bytes());
        result.extend(&self.version.to_le_bytes());
        
        result
    }
    
    fn from_stored_value(key: Vec<u8>, stored: &[u8]) -> Option<Self> {
        if stored.len() < 4 {
            return None;
        }
        
        // Parse stored format
        let value_len = u32::from_le_bytes([stored[0], stored[1], stored[2], stored[3]]) as usize;
        
        if stored.len() < 4 + value_len + 32 + 8 + 8 {
            return None;
        }
        
        let value = stored[4..4 + value_len].to_vec();
        let checksum = stored[4 + value_len..4 + value_len + 32].to_vec();
        let timestamp = u64::from_le_bytes([
            stored[4 + value_len + 32],
            stored[4 + value_len + 33],
            stored[4 + value_len + 34],
            stored[4 + value_len + 35],
            stored[4 + value_len + 36],
            stored[4 + value_len + 37],
            stored[4 + value_len + 38],
            stored[4 + value_len + 39],
        ]);
        let version = u64::from_le_bytes([
            stored[4 + value_len + 40],
            stored[4 + value_len + 41],
            stored[4 + value_len + 42],
            stored[4 + value_len + 43],
            stored[4 + value_len + 44],
            stored[4 + value_len + 45],
            stored[4 + value_len + 46],
            stored[4 + value_len + 47],
        ]);
        
        Some(Self {
            key,
            value,
            checksum,
            timestamp,
            version,
        })
    }
}

struct IntegrityChecker {
    db: Arc<Database>,
    shadow_data: Arc<Mutex<HashMap<Vec<u8>, DataRecord>>>,
    version_counter: Arc<Mutex<u64>>,
    anomalies: Arc<Mutex<Vec<String>>>,
}

impl IntegrityChecker {
    fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            shadow_data: Arc::new(Mutex::new(HashMap::new())),
            version_counter: Arc::new(Mutex::new(0)),
            anomalies: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    /// Write data with paranoid integrity checks
    fn write_paranoid(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        // Get next version
        let version = {
            let mut v = self.version_counter.lock().unwrap();
            *v += 1;
            *v
        };
        
        // Create record with integrity info
        let record = DataRecord::new(key.to_vec(), value.to_vec(), version);
        
        // Verify checksum immediately
        if !record.verify() {
            return Err("Checksum verification failed immediately after creation".to_string());
        }
        
        // Store in database
        let stored_value = record.to_stored_value();
        self.db.put(key, &stored_value)
            .map_err(|e| format!("Database write failed: {}", e))?;
        
        // Immediately read back and verify
        match self.db.get(key) {
            Ok(Some(read_value)) => {
                if read_value != stored_value {
                    return Err("Read-after-write verification failed".to_string());
                }
            }
            Ok(None) => return Err("Key not found immediately after write".to_string()),
            Err(e) => return Err(format!("Read-after-write failed: {}", e)),
        }
        
        // Store in shadow copy
        self.shadow_data.lock().unwrap().insert(key.to_vec(), record);
        
        Ok(())
    }
    
    /// Read data with paranoid integrity checks
    fn read_paranoid(&self, key: &[u8]) -> Result<Vec<u8>, String> {
        // Read from database
        let stored_value = self.db.get(key)
            .map_err(|e| format!("Database read failed: {}", e))?
            .ok_or_else(|| "Key not found".to_string())?;
        
        // Parse and verify
        let record = DataRecord::from_stored_value(key.to_vec(), &stored_value)
            .ok_or_else(|| "Failed to parse stored record".to_string())?;
        
        // Verify checksum
        if !record.verify() {
            self.record_anomaly(format!("Checksum mismatch for key: {:?}", key));
            return Err("Checksum verification failed".to_string());
        }
        
        // Verify against shadow copy if available
        if let Some(shadow_record) = self.shadow_data.lock().unwrap().get(key) {
            if shadow_record.value != record.value {
                self.record_anomaly(format!("Shadow data mismatch for key: {:?}", key));
                return Err("Shadow data verification failed".to_string());
            }
            
            if shadow_record.version > record.version {
                self.record_anomaly(format!("Version regression for key: {:?}", key));
                return Err("Version verification failed".to_string());
            }
        }
        
        Ok(record.value)
    }
    
    /// Perform comprehensive integrity scan
    fn full_integrity_scan(&self) -> ParanoiaIntegrityReport {
        let start_time = Instant::now();
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut checked_keys = 0;
        let mut corrupted_keys = 0;
        
        // Check all keys in shadow data
        let shadow_copy = self.shadow_data.lock().unwrap().clone();
        for (key, shadow_record) in shadow_copy {
            checked_keys += 1;
            
            // Read from database
            match self.db.get(&key) {
                Ok(Some(stored_value)) => {
                    // Parse record
                    match DataRecord::from_stored_value(key.clone(), &stored_value) {
                        Some(record) => {
                            // Verify checksum
                            if !record.verify() {
                                errors.push(format!("Checksum mismatch for key: {:?}", key));
                                corrupted_keys += 1;
                            }
                            
                            // Verify against shadow
                            if record.value != shadow_record.value {
                                errors.push(format!("Value mismatch for key: {:?}", key));
                                corrupted_keys += 1;
                            }
                            
                            if record.version != shadow_record.version {
                                warnings.push(format!("Version mismatch for key: {:?}", key));
                            }
                            
                            if record.timestamp > shadow_record.timestamp + 1000000 {
                                warnings.push(format!("Timestamp drift for key: {:?}", key));
                            }
                        }
                        None => {
                            errors.push(format!("Failed to parse record for key: {:?}", key));
                            corrupted_keys += 1;
                        }
                    }
                }
                Ok(None) => {
                    errors.push(format!("Key missing from database: {:?}", key));
                    corrupted_keys += 1;
                }
                Err(e) => {
                    errors.push(format!("Failed to read key {:?}: {}", key, e));
                    corrupted_keys += 1;
                }
            }
        }
        
        ParanoiaIntegrityReport {
            checked_items: checked_keys,
            errors,
            warnings,
            corrupted_keys,
            duration: start_time.elapsed(),
        }
    }
    
    /// Perform continuous background verification
    fn start_background_verification(&self) {
        let db = self.db.clone();
        let shadow_data = self.shadow_data.clone();
        let anomalies = self.anomalies.clone();
        
        thread::spawn(move || {
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            
            loop {
                thread::sleep(Duration::from_millis(100));
                
                // Sample random keys for verification
                let keys: Vec<Vec<u8>> = {
                    let shadow = shadow_data.lock().unwrap();
                    if shadow.is_empty() {
                        continue;
                    }
                    
                    shadow.keys()
                        .filter(|_| rng.gen_ratio(1, 10)) // Sample 10%
                        .take(100)
                        .cloned()
                        .collect()
                };
                
                for key in keys {
                    // Read and verify
                    match db.get(&key) {
                        Ok(Some(stored_value)) => {
                            if let Some(record) = DataRecord::from_stored_value(key.clone(), &stored_value) {
                                if !record.verify() {
                                    let msg = format!("Background check: checksum mismatch for key: {:?}", key);
                                    anomalies.lock().unwrap().push(msg);
                                }
                            }
                        }
                        Ok(None) => {
                            let msg = format!("Background check: key disappeared: {:?}", key);
                            anomalies.lock().unwrap().push(msg);
                        }
                        Err(e) => {
                            let msg = format!("Background check: read error for key {:?}: {}", key, e);
                            anomalies.lock().unwrap().push(msg);
                        }
                    }
                }
            }
        });
    }
    
    fn record_anomaly(&self, anomaly: String) {
        println!("⚠️  ANOMALY: {}", anomaly);
        self.anomalies.lock().unwrap().push(anomaly);
    }
    
    fn get_anomalies(&self) -> Vec<String> {
        self.anomalies.lock().unwrap().clone()
    }
}

fn main() {
    println!("🔒 Lightning DB Data Integrity Paranoia Test");
    println!("🛡️  Multiple layers of data validation and verification");
    println!("{}", "=".repeat(70));
    
    // Create database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("paranoia_db");
    
    let config = LightningDbConfig {
        cache_size: 16 * 1024 * 1024, // 16MB
        compression_enabled: true,
        use_improved_wal: true,
        ..Default::default()
    };
    
    let db = Arc::new(Database::create(&db_path, config).unwrap());
    let checker = IntegrityChecker::new(db.clone());
    
    // Start background verification
    checker.start_background_verification();
    
    println!("\n📝 Phase 1: Writing data with paranoid checks...");
    let start_time = Instant::now();
    
    // Write test data
    let mut write_errors = 0;
    for i in 0..1000 {
        let key = format!("paranoid_key_{:06}", i);
        let value = format!("paranoid_value_{:06}_data_{}", i, "x".repeat(100));
        
        match checker.write_paranoid(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                if i % 100 == 0 {
                    println!("   Written {} records...", i + 1);
                }
            }
            Err(e) => {
                println!("   ❌ Write error at {}: {}", i, e);
                write_errors += 1;
            }
        }
    }
    
    println!("   ✅ Write phase complete: {} errors", write_errors);
    
    println!("\n📖 Phase 2: Reading data with paranoid checks...");
    
    // Read and verify all data
    let mut read_errors = 0;
    for i in 0..1000 {
        let key = format!("paranoid_key_{:06}", i);
        
        match checker.read_paranoid(key.as_bytes()) {
            Ok(value) => {
                let expected = format!("paranoid_value_{:06}_data_{}", i, "x".repeat(100));
                if value != expected.as_bytes() {
                    println!("   ❌ Value mismatch at {}", i);
                    read_errors += 1;
                }
            }
            Err(e) => {
                println!("   ❌ Read error at {}: {}", i, e);
                read_errors += 1;
            }
        }
    }
    
    println!("   ✅ Read phase complete: {} errors", read_errors);
    
    println!("\n🔍 Phase 3: Full integrity scan...");
    let integrity_report = checker.full_integrity_scan();
    
    println!("   Checked items: {}", integrity_report.checked_items);
    println!("   Critical errors: {}", integrity_report.errors.len());
    println!("   Scan duration: {:?}", integrity_report.duration);
    
    if !integrity_report.errors.is_empty() {
        println!("\n   Critical errors found:");
        for error in &integrity_report.errors {
            println!("      - {}", error);
        }
    }
    
    // Simulate corruption
    println!("\n💣 Phase 4: Simulating data corruption...");
    
    // Corrupt some data directly
    let corrupt_key = b"paranoid_key_000500";
    db.put(corrupt_key, b"corrupted_data").unwrap();
    
    println!("   Corrupted key: paranoid_key_000500");
    
    // Try to read corrupted data
    match checker.read_paranoid(corrupt_key) {
        Ok(_) => println!("   ❌ CRITICAL: Corruption not detected!"),
        Err(e) => println!("   ✅ Corruption detected: {}", e),
    }
    
    // Wait for background checks
    thread::sleep(Duration::from_secs(2));
    
    println!("\n🚨 Phase 5: Anomaly report...");
    let anomalies = checker.get_anomalies();
    
    if anomalies.is_empty() {
        println!("   ✅ No anomalies detected");
    } else {
        println!("   ⚠️  {} anomalies detected:", anomalies.len());
        for anomaly in anomalies.iter().take(10) {
            println!("      - {}", anomaly);
        }
        if anomalies.len() > 10 {
            println!("      ... and {} more", anomalies.len() - 10);
        }
    }
    
    // Final verdict
    println!("\n{}", "=".repeat(70));
    println!("🏁 DATA INTEGRITY VERDICT:");
    
    let total_errors = write_errors + read_errors + integrity_report.errors.len() + integrity_report.corrupted_keys;
    let elapsed = start_time.elapsed();
    
    if total_errors == 0 && anomalies.is_empty() {
        println!("   ✅ PERFECT - All integrity checks passed");
        println!("   No data corruption or anomalies detected");
    } else if total_errors < 5 && anomalies.len() < 10 {
        println!("   ⚠️  GOOD - Minor issues detected");
        println!("   {} total errors, {} anomalies", total_errors, anomalies.len());
    } else {
        println!("   ❌ CRITICAL - Significant integrity issues");
        println!("   {} total errors, {} anomalies", total_errors, anomalies.len());
    }
    
    println!("\n📊 Performance:");
    println!("   Operations: 2000 (1000 writes + 1000 reads)");
    println!("   Duration: {:?}", elapsed);
    println!("   Throughput: {:.0} ops/sec", 2000.0 / elapsed.as_secs_f64());
    
    println!("\n{}", "=".repeat(70));
}