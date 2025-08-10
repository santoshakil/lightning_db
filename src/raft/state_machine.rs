use super::*;
// use crate::db::Database;
// use crate::transaction::{Transaction, TransactionManager};
use bincode::{config, decode_from_slice, encode_to_vec, Decode, Encode};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Lightning DB state machine implementation
pub struct LightningStateMachine {
    /// The underlying database
    db: Arc<mock::Database>,

    /// Transaction manager
    tx_manager: Arc<mock::MockTransactionManager>,

    /// Applied transaction IDs for deduplication
    applied_txs: Arc<RwLock<HashMap<(u64, u64), Vec<u8>>>>, // (client_id, request_id) -> result

    /// Configuration
    config: StateMachineConfig,
}

/// State machine configuration
#[derive(Debug, Clone)]
pub struct StateMachineConfig {
    /// Enable transaction deduplication
    pub deduplication: bool,

    /// Maximum number of cached results
    pub max_cached_results: usize,

    /// Enable deterministic execution
    pub deterministic: bool,
}

impl Default for StateMachineConfig {
    fn default() -> Self {
        Self {
            deduplication: true,
            max_cached_results: 10000,
            deterministic: true,
        }
    }
}

impl LightningStateMachine {
    /// Create a new state machine
    pub fn new(db: Arc<mock::Database>, config: StateMachineConfig) -> Self {
        let tx_manager = Arc::new(mock::MockTransactionManager::new());

        Self {
            db,
            tx_manager,
            applied_txs: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Check if a transaction has been applied
    fn check_duplicate(&self, client_id: u64, request_id: u64) -> Option<Vec<u8>> {
        if !self.config.deduplication {
            return None;
        }

        self.applied_txs
            .read()
            .get(&(client_id, request_id))
            .cloned()
    }

    /// Record applied transaction
    fn record_applied(&self, client_id: u64, request_id: u64, result: Vec<u8>) {
        if !self.config.deduplication {
            return;
        }

        let mut applied = self.applied_txs.write();

        // Evict old entries if needed
        if applied.len() >= self.config.max_cached_results {
            // Simple FIFO eviction
            if let Some(first_key) = applied.keys().next().cloned() {
                applied.remove(&first_key);
            }
        }

        applied.insert((client_id, request_id), result);
    }
}

impl StateMachine for LightningStateMachine {
    fn apply(&self, command: &Command) -> Result<Vec<u8>> {
        match command {
            Command::Write { key, value } => {
                // Simple write operation
                self.db.put(key, value)?;
                Ok(vec![1]) // Success indicator
            }

            Command::Delete { key } => {
                // Delete operation
                self.db.delete(key)?;
                Ok(vec![1]) // Success indicator
            }

            Command::Transaction { ops } => {
                // Execute transaction
                let mut tx = mock::MockTransaction::new();
                let mut results = Vec::new();

                for op in ops {
                    match op {
                        TransactionOp::Write { key, value } => {
                            tx.write(key.clone(), value.clone());
                            results.push(1); // Success
                        }
                        TransactionOp::Delete { key } => {
                            tx.delete(key.clone());
                            results.push(1); // Success
                        }
                        TransactionOp::Read { key } => {
                            // Reads are handled separately for linearizability
                            if let Ok(value) = self.db.get(key) {
                                if let Some(v) = value {
                                    results.extend_from_slice(&(v.len() as u32).to_be_bytes());
                                    results.extend_from_slice(&v);
                                } else {
                                    results.extend_from_slice(&0u32.to_be_bytes());
                                }
                            } else {
                                results.extend_from_slice(&0u32.to_be_bytes());
                            }
                        }
                    }
                }

                // Commit transaction
                self.tx_manager.execute(tx, &self.db)?;

                Ok(results)
            }

            Command::ConfigChange(change) => {
                // Configuration changes are handled by Raft layer
                // State machine just acknowledges
                match change {
                    ConfigChange::AddNode(node_id, address) => {
                        println!("State machine: Adding node {} at {}", node_id, address);
                    }
                    ConfigChange::RemoveNode(node_id) => {
                        println!("State machine: Removing node {}", node_id);
                    }
                    ConfigChange::UpdateNode(node_id, address) => {
                        println!("State machine: Updating node {} to {}", node_id, address);
                    }
                }
                Ok(vec![1]) // Success
            }

            Command::NoOp => {
                // No operation, used for leader establishment
                Ok(vec![])
            }
        }
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key)
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        // Create a consistent snapshot of the database
        let snapshot_data = self.db.create_snapshot()?;

        // Include metadata
        let metadata = SnapshotMetadata {
            version: 1,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            size: snapshot_data.len() as u64,
        };

        // Serialize metadata + data
        let mut result = encode_to_vec(&metadata, config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        result.extend_from_slice(&snapshot_data);

        Ok(result)
    }

    fn restore(&self, snapshot: &[u8]) -> Result<()> {
        // Deserialize metadata
        let metadata_size = std::mem::size_of::<SnapshotMetadata>();
        if snapshot.len() < metadata_size {
            return Err(Error::InvalidData("Snapshot too small".to_string()));
        }

        let (metadata, _): (SnapshotMetadata, usize) =
            decode_from_slice(&snapshot[..metadata_size], config::standard())
                .map_err(|e| Error::Serialization(e.to_string()))?;

        // Verify version
        if metadata.version != 1 {
            return Err(Error::InvalidData(format!(
                "Unsupported snapshot version: {}",
                metadata.version
            )));
        }

        // Restore database from snapshot data
        self.db.restore_snapshot(&snapshot[metadata_size..])?;

        // Clear applied transactions cache
        self.applied_txs.write().clear();

        Ok(())
    }
}

/// Snapshot metadata
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct SnapshotMetadata {
    version: u32,
    timestamp: u64,
    size: u64,
}

/// Wrapper for applying commands with deduplication
impl LightningStateMachine {
    /// Apply command with deduplication support
    pub fn apply_with_dedup(
        &self,
        command: &Command,
        client_id: Option<u64>,
        request_id: Option<u64>,
    ) -> Result<Vec<u8>> {
        // Check for duplicate
        if let (Some(cid), Some(rid)) = (client_id, request_id) {
            if let Some(cached_result) = self.check_duplicate(cid, rid) {
                return Ok(cached_result);
            }
        }

        // Apply command
        let result = self.apply(command)?;

        // Record result
        if let (Some(cid), Some(rid)) = (client_id, request_id) {
            self.record_applied(cid, rid, result.clone());
        }

        Ok(result)
    }
}

/// Mock implementations for Database and TransactionManager
/// These would be replaced with actual Lightning DB implementations
mod mock {
    use super::*;
    use std::collections::BTreeMap;

    pub struct Database {
        data: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    }

    impl Database {
        pub fn new() -> Self {
            Self {
                data: RwLock::new(BTreeMap::new()),
            }
        }

        pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
            self.data.write().insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.data.read().get(key).cloned())
        }

        pub fn delete(&self, key: &[u8]) -> Result<()> {
            self.data.write().remove(key);
            Ok(())
        }

        pub fn create_snapshot(&self) -> Result<Vec<u8>> {
            let data = self.data.read();
            encode_to_vec(&*data, config::standard())
                .map_err(|e| Error::Serialization(e.to_string()))
        }

        pub fn restore_snapshot(&self, snapshot: &[u8]) -> Result<()> {
            let (new_data, _): (BTreeMap<Vec<u8>, Vec<u8>>, usize) =
                decode_from_slice(snapshot, config::standard())
                    .map_err(|e| Error::Serialization(e.to_string()))?;
            *self.data.write() = new_data;
            Ok(())
        }
    }

    pub struct MockTransaction {
        writes: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    }

    impl MockTransaction {
        pub fn new() -> Self {
            Self {
                writes: Vec::new(),
                deletes: Vec::new(),
            }
        }

        pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) {
            self.writes.push((key, value));
        }

        pub fn delete(&mut self, key: Vec<u8>) {
            self.deletes.push(key);
        }
    }

    pub struct MockTransactionManager;

    impl MockTransactionManager {
        pub fn new() -> Self {
            Self
        }

        pub fn execute(&self, tx: MockTransaction, db: &Database) -> Result<()> {
            // Simple non-atomic execution for mock
            for (key, value) in tx.writes {
                db.put(&key, &value)?;
            }
            for key in tx.deletes {
                db.delete(&key)?;
            }
            Ok(())
        }
    }
}

// Re-export mock implementations for testing
#[cfg(test)]
pub use mock::{
    Database, MockTransaction as Transaction, MockTransactionManager as TransactionManager,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_machine_write() {
        let db = Arc::new(Database::new());
        let sm = LightningStateMachine::new(db.clone(), StateMachineConfig::default());

        let command = Command::Write {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };

        let result = sm.apply(&command).unwrap();
        assert_eq!(result, vec![1]);

        let value = sm.read(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_deduplication() {
        let db = Arc::new(Database::new());
        let sm = LightningStateMachine::new(db.clone(), StateMachineConfig::default());

        let command = Command::Write {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };

        // Apply with deduplication
        let result1 = sm.apply_with_dedup(&command, Some(1), Some(1)).unwrap();

        // Apply same command again - should return cached result
        let result2 = sm.apply_with_dedup(&command, Some(1), Some(1)).unwrap();
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_snapshot_restore() {
        let db = Arc::new(Database::new());
        let sm = LightningStateMachine::new(db.clone(), StateMachineConfig::default());

        // Add some data
        sm.apply(&Command::Write {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();

        sm.apply(&Command::Write {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        })
        .unwrap();

        // Create snapshot
        let snapshot = sm.snapshot().unwrap();

        // Create new state machine and restore
        let db2 = Arc::new(Database::new());
        let sm2 = LightningStateMachine::new(db2.clone(), StateMachineConfig::default());
        sm2.restore(&snapshot).unwrap();

        // Verify data
        assert_eq!(sm2.read(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(sm2.read(b"key2").unwrap(), Some(b"value2".to_vec()));
    }
}
