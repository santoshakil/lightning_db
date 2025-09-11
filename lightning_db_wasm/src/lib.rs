use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::js_sys::Promise;

// Using the default allocator; remove unused wee_alloc cfg to avoid warnings.

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

/// Lightning DB WASM wrapper
#[wasm_bindgen]
pub struct LightningDB {
    // In WASM, we simulate the database in memory
    // In a real implementation, this would use IndexedDB or OPFS
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    transactions: Arc<Mutex<HashMap<u64, Transaction>>>,
    next_tx_id: Arc<Mutex<u64>>,
}

#[derive(Clone)]
struct Transaction {
    #[allow(dead_code)]
    id: u64,
    operations: Vec<Operation>,
    committed: bool,
}

#[derive(Clone)]
enum Operation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

#[wasm_bindgen]
#[derive(Serialize, Deserialize)]
pub struct Config {
    cache_size: u32,
    compression_enabled: bool,
    sync_mode: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cache_size: 100 * 1024 * 1024, // 100MB
            compression_enabled: true,
            sync_mode: "async".to_string(),
        }
    }
}

#[wasm_bindgen]
impl Config {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    #[wasm_bindgen(getter)]
    pub fn cache_size(&self) -> u32 {
        self.cache_size
    }

    #[wasm_bindgen(setter)]
    pub fn set_cache_size(&mut self, value: u32) {
        self.cache_size = value;
    }

    #[wasm_bindgen(getter)]
    pub fn compression_enabled(&self) -> bool {
        self.compression_enabled
    }

    #[wasm_bindgen(setter)]
    pub fn set_compression_enabled(&mut self, value: bool) {
        self.compression_enabled = value;
    }

    #[wasm_bindgen(getter)]
    pub fn sync_mode(&self) -> String {
        self.sync_mode.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_sync_mode(&mut self, value: String) {
        self.sync_mode = value;
    }
}

#[wasm_bindgen]
#[derive(Serialize, Deserialize)]
pub struct Stats {
    pub total_keys: u32,
    pub total_size: u32,
    pub cache_hit_rate: f32,
    pub compression_ratio: f32,
}

#[wasm_bindgen]
impl LightningDB {
    /// Create a new Lightning DB instance
    #[wasm_bindgen(constructor)]
    pub fn new(config: &Config) -> Result<LightningDB, JsValue> {
        console_error_panic_hook::set_once();

        console_log!(
            "Creating Lightning DB with config: cache_size={}, compression={}, sync_mode={}",
            config.cache_size,
            config.compression_enabled,
            config.sync_mode
        );

        Ok(LightningDB {
            data: Arc::new(Mutex::new(HashMap::new())),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            next_tx_id: Arc::new(Mutex::new(1)),
        })
    }

    /// Put a key-value pair
    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), JsValue> {
        let mut data = self.data.lock().unwrap();
        data.insert(key.to_string(), value.to_vec());
        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, JsValue> {
        let data = self.data.lock().unwrap();
        Ok(data.get(key).cloned())
    }

    /// Delete a key
    pub fn delete(&self, key: &str) -> Result<(), JsValue> {
        let mut data = self.data.lock().unwrap();
        data.remove(key);
        Ok(())
    }

    /// Begin a transaction
    pub fn begin_transaction(&self) -> Result<u64, JsValue> {
        let mut next_id = self.next_tx_id.lock().unwrap();
        let tx_id = *next_id;
        *next_id += 1;

        let mut transactions = self.transactions.lock().unwrap();
        transactions.insert(
            tx_id,
            Transaction {
                id: tx_id,
                operations: Vec::new(),
                committed: false,
            },
        );

        Ok(tx_id)
    }

    /// Put within a transaction
    pub fn tx_put(&self, tx_id: u64, key: &str, value: &[u8]) -> Result<(), JsValue> {
        let mut transactions = self.transactions.lock().unwrap();
        let tx = transactions
            .get_mut(&tx_id)
            .ok_or_else(|| JsValue::from_str("Transaction not found"))?;

        if tx.committed {
            return Err(JsValue::from_str("Transaction already committed"));
        }

        tx.operations.push(Operation::Put {
            key: key.to_string(),
            value: value.to_vec(),
        });

        Ok(())
    }

    /// Delete within a transaction
    pub fn tx_delete(&self, tx_id: u64, key: &str) -> Result<(), JsValue> {
        let mut transactions = self.transactions.lock().unwrap();
        let tx = transactions
            .get_mut(&tx_id)
            .ok_or_else(|| JsValue::from_str("Transaction not found"))?;

        if tx.committed {
            return Err(JsValue::from_str("Transaction already committed"));
        }

        tx.operations.push(Operation::Delete {
            key: key.to_string(),
        });

        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, tx_id: u64) -> Result<(), JsValue> {
        let mut transactions = self.transactions.lock().unwrap();
        let tx = transactions
            .get_mut(&tx_id)
            .ok_or_else(|| JsValue::from_str("Transaction not found"))?;

        if tx.committed {
            return Err(JsValue::from_str("Transaction already committed"));
        }

        // Apply all operations
        let mut data = self.data.lock().unwrap();
        for op in &tx.operations {
            match op {
                Operation::Put { key, value } => {
                    data.insert(key.clone(), value.clone());
                }
                Operation::Delete { key } => {
                    data.remove(key);
                }
            }
        }

        tx.committed = true;
        transactions.remove(&tx_id);

        Ok(())
    }

    /// Rollback a transaction
    pub fn rollback_transaction(&self, tx_id: u64) -> Result<(), JsValue> {
        let mut transactions = self.transactions.lock().unwrap();
        transactions.remove(&tx_id);
        Ok(())
    }

    /// Get all keys with a prefix
    pub fn keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>, JsValue> {
        let data = self.data.lock().unwrap();
        let keys: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        Ok(keys)
    }

    /// Get database statistics
    pub fn get_stats(&self) -> Result<Stats, JsValue> {
        let data = self.data.lock().unwrap();
        let total_size: usize = data.values().map(|v| v.len()).sum();

        Ok(Stats {
            total_keys: data.len() as u32,
            total_size: total_size as u32,
            cache_hit_rate: 0.95,   // Simulated
            compression_ratio: 0.5, // Simulated
        })
    }

    /// Clear all data
    pub fn clear(&self) -> Result<(), JsValue> {
        let mut data = self.data.lock().unwrap();
        data.clear();
        Ok(())
    }
}

// Async versions using promises
#[wasm_bindgen]
impl LightningDB {
    /// Async put operation
    pub fn put_async(&self, key: String, value: Vec<u8>) -> Promise {
        let db = self.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            db.put(&key, &value)?;
            Ok(JsValue::undefined())
        })
    }

    /// Async get operation
    pub fn get_async(&self, key: String) -> Promise {
        let db = self.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            match db.get(&key)? {
                Some(value) => Ok(JsValue::from(value)),
                None => Ok(JsValue::null()),
            }
        })
    }

    /// Async delete operation
    pub fn delete_async(&self, key: String) -> Promise {
        let db = self.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            db.delete(&key)?;
            Ok(JsValue::undefined())
        })
    }
}

// Implement Clone manually to make it available in WASM
impl Clone for LightningDB {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            transactions: Arc::clone(&self.transactions),
            next_tx_id: Arc::clone(&self.next_tx_id),
        }
    }
}

// Helper functions for JavaScript
#[wasm_bindgen]
pub fn string_to_bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

#[wasm_bindgen]
pub fn bytes_to_string(bytes: &[u8]) -> Result<String, JsValue> {
    String::from_utf8(bytes.to_vec()).map_err(|e| JsValue::from_str(&format!("UTF-8 error: {}", e)))
}

// Benchmark utilities
#[wasm_bindgen]
pub struct BenchmarkResult {
    pub operations: u32,
    pub duration_ms: f64,
    pub ops_per_sec: f64,
    pub avg_latency_us: f64,
}

#[wasm_bindgen]
pub fn benchmark_writes(db: &LightningDB, count: u32) -> Result<BenchmarkResult, JsValue> {
    let start = web_sys::window()
        .ok_or_else(|| JsValue::from_str("No window"))?
        .performance()
        .ok_or_else(|| JsValue::from_str("No performance"))?
        .now();

    for i in 0..count {
        let key = format!("bench_key_{}", i);
        let value = format!("bench_value_{}", i);
        db.put(&key, value.as_bytes())?;
    }

    let end = web_sys::window()
        .ok_or_else(|| JsValue::from_str("No window"))?
        .performance()
        .ok_or_else(|| JsValue::from_str("No performance"))?
        .now();

    let duration_ms = end - start;
    let ops_per_sec = (count as f64 / duration_ms) * 1000.0;
    let avg_latency_us = (duration_ms * 1000.0) / count as f64;

    Ok(BenchmarkResult {
        operations: count,
        duration_ms,
        ops_per_sec,
        avg_latency_us,
    })
}

#[wasm_bindgen]
pub fn benchmark_reads(db: &LightningDB, count: u32) -> Result<BenchmarkResult, JsValue> {
    let start = web_sys::window()
        .ok_or_else(|| JsValue::from_str("No window"))?
        .performance()
        .ok_or_else(|| JsValue::from_str("No performance"))?
        .now();

    for i in 0..count {
        let key = format!("bench_key_{}", i);
        let _ = db.get(&key)?;
    }

    let end = web_sys::window()
        .ok_or_else(|| JsValue::from_str("No window"))?
        .performance()
        .ok_or_else(|| JsValue::from_str("No performance"))?
        .now();

    let duration_ms = end - start;
    let ops_per_sec = (count as f64 / duration_ms) * 1000.0;
    let avg_latency_us = (duration_ms * 1000.0) / count as f64;

    Ok(BenchmarkResult {
        operations: count,
        duration_ms,
        ops_per_sec,
        avg_latency_us,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn test_basic_operations() {
        let config = Config::new();
        let db = LightningDB::new(&config).unwrap();

        // Put and get
        db.put("key1", b"value1").unwrap();
        let value = db.get("key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Delete
        db.delete("key1").unwrap();
        let value = db.get("key1").unwrap();
        assert_eq!(value, None);
    }

    #[wasm_bindgen_test]
    fn test_transactions() {
        let config = Config::new();
        let db = LightningDB::new(&config).unwrap();

        let tx_id = db.begin_transaction().unwrap();
        db.tx_put(tx_id, "tx_key", b"tx_value").unwrap();

        // Value shouldn't be visible before commit
        assert_eq!(db.get("tx_key").unwrap(), None);

        db.commit_transaction(tx_id).unwrap();

        // Value should be visible after commit
        assert_eq!(db.get("tx_key").unwrap(), Some(b"tx_value".to_vec()));
    }
}
