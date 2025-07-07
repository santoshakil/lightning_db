mod enhanced;

pub use enhanced::{EnhancedReplicationManager, ReplicationMessage, ReplicationStats};

use crate::error::{Error, Result};
use crate::wal::WALOperation;
use crate::Database;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplicationRole {
    Master,
    Slave,
}

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub role: ReplicationRole,
    pub bind_address: String,
    pub master_address: Option<String>,
    pub sync_interval_ms: u64,
    pub batch_size: usize,
    pub heartbeat_interval_ms: u64,
    pub snapshot_threshold: u64,
    pub conflict_resolution: ConflictResolution,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ConflictResolution {
    LastWriteWins,
    MasterWins,
    SlaveWins,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: ReplicationRole::Master,
            bind_address: "127.0.0.1:7890".to_string(),
            master_address: None,
            sync_interval_ms: 100,
            batch_size: 1000,
            heartbeat_interval_ms: 5000,
            snapshot_threshold: 10000,
            conflict_resolution: ConflictResolution::LastWriteWins,
        }
    }
}

pub struct ReplicationManager {
    config: ReplicationConfig,
    database: Arc<Database>,
    is_running: Arc<AtomicBool>,
    last_synced_lsn: Arc<AtomicU64>,
    pending_operations: Arc<RwLock<VecDeque<WALOperation>>>,
    sync_condvar: Arc<(Mutex<bool>, Condvar)>,
    #[allow(dead_code)]
    last_heartbeat: Arc<RwLock<Instant>>,
    #[allow(dead_code)]
    slave_connections: Arc<RwLock<Vec<TcpStream>>>,
}

impl ReplicationManager {
    pub fn new(config: ReplicationConfig, database: Arc<Database>) -> Self {
        Self {
            config,
            database,
            is_running: Arc::new(AtomicBool::new(false)),
            last_synced_lsn: Arc::new(AtomicU64::new(0)),
            pending_operations: Arc::new(RwLock::new(VecDeque::new())),
            sync_condvar: Arc::new((Mutex::new(false), Condvar::new())),
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            slave_connections: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn start(&self) -> Result<()> {
        if self.is_running.load(Ordering::Relaxed) {
            return Err(Error::Generic("Replication already running".to_string()));
        }

        self.is_running.store(true, Ordering::Relaxed);

        match self.config.role {
            ReplicationRole::Master => self.start_master()?,
            ReplicationRole::Slave => self.start_slave()?,
        }

        Ok(())
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);

        // Wake up any waiting threads
        let (lock, cvar) = &*self.sync_condvar;
        let mut pending = lock.lock().unwrap();
        *pending = true;
        cvar.notify_all();
    }

    fn start_master(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.bind_address)
            .map_err(|e| Error::Generic(format!("Failed to bind: {}", e)))?;

        listener
            .set_nonblocking(true)
            .map_err(|e| Error::Generic(format!("Failed to set nonblocking: {}", e)))?;

        let is_running = self.is_running.clone();
        let pending_operations = self.pending_operations.clone();
        let sync_interval = Duration::from_millis(self.config.sync_interval_ms);

        thread::spawn(move || {
            let mut slaves = Vec::new();

            while is_running.load(Ordering::Relaxed) {
                // Accept new slave connections
                match listener.accept() {
                    Ok((stream, addr)) => {
                        println!("New slave connected from: {}", addr);
                        slaves.push(stream);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No new connections, continue
                    }
                    Err(e) => {
                        eprintln!("Error accepting connection: {}", e);
                    }
                }

                // Send pending operations to all slaves
                let ops = {
                    let mut pending = pending_operations.write();
                    let ops: Vec<_> = pending.drain(..).collect();
                    ops
                };

                if !ops.is_empty() {
                    let mut disconnected = Vec::new();

                    for (idx, slave) in slaves.iter_mut().enumerate() {
                        if let Err(e) = Self::send_operations(slave, &ops) {
                            eprintln!("Failed to send to slave {}: {}", idx, e);
                            disconnected.push(idx);
                        }
                    }

                    // Remove disconnected slaves
                    for idx in disconnected.into_iter().rev() {
                        slaves.swap_remove(idx);
                    }
                }

                thread::sleep(sync_interval);
            }
        });

        Ok(())
    }

    fn start_slave(&self) -> Result<()> {
        let master_addr = self
            .config
            .master_address
            .as_ref()
            .ok_or_else(|| Error::Generic("Master address not configured".to_string()))?
            .clone();

        let stream = TcpStream::connect(&master_addr)
            .map_err(|e| Error::Generic(format!("Failed to connect to master: {}", e)))?;

        stream
            .set_nonblocking(false)
            .map_err(|e| Error::Generic(format!("Failed to set blocking: {}", e)))?;

        let is_running = self.is_running.clone();
        let database = self.database.clone();
        let last_synced_lsn = self.last_synced_lsn.clone();

        thread::spawn(move || {
            Self::slave_thread(stream, master_addr, is_running, database, last_synced_lsn);
        });

        Ok(())
    }

    fn slave_thread(
        mut stream: TcpStream,
        master_addr: String,
        is_running: Arc<AtomicBool>,
        database: Arc<Database>,
        last_synced_lsn: Arc<AtomicU64>,
    ) {
        while is_running.load(Ordering::Relaxed) {
            match Self::receive_operations(&mut stream) {
                Ok(operations) => {
                    for op in operations {
                        if let Err(e) = Self::apply_operation(&database, &op) {
                            eprintln!("Failed to apply operation: {}", e);
                        }
                    }

                    // Update LSN
                    last_synced_lsn.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("Error receiving operations: {}", e);

                    // Try to reconnect
                    thread::sleep(Duration::from_secs(5));

                    if let Ok(new_stream) = TcpStream::connect(&master_addr) {
                        stream = new_stream;
                        println!("Reconnected to master");
                    }
                }
            }
        }
    }

    fn send_operations(stream: &mut TcpStream, operations: &[WALOperation]) -> Result<()> {
        // Simple protocol: length (4 bytes) + serialized operations
        let serialized = bincode::encode_to_vec(operations, bincode::config::standard())
            .map_err(|e| Error::Generic(format!("Serialization error: {}", e)))?;

        let len = serialized.len() as u32;
        stream
            .write_all(&len.to_be_bytes())
            .map_err(|e| Error::Generic(format!("Write error: {}", e)))?;

        stream
            .write_all(&serialized)
            .map_err(|e| Error::Generic(format!("Write error: {}", e)))?;

        stream
            .flush()
            .map_err(|e| Error::Generic(format!("Flush error: {}", e)))?;

        Ok(())
    }

    fn receive_operations(stream: &mut TcpStream) -> Result<Vec<WALOperation>> {
        // Read length
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .map_err(|e| Error::Generic(format!("Read error: {}", e)))?;

        let len = u32::from_be_bytes(len_buf) as usize;

        // Read data
        let mut data = vec![0u8; len];
        stream
            .read_exact(&mut data)
            .map_err(|e| Error::Generic(format!("Read error: {}", e)))?;

        let operations: Vec<WALOperation> =
            bincode::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| Error::Generic(format!("Deserialization error: {}", e)))?
                .0;

        Ok(operations)
    }

    fn apply_operation(database: &Database, operation: &WALOperation) -> Result<()> {
        match operation {
            WALOperation::Put { key, value } => {
                database.put(key, value)?;
            }
            WALOperation::Delete { key } => {
                database.delete(key)?;
            }
            WALOperation::TransactionBegin { .. } => {
                // Transactions are handled locally
            }
            WALOperation::TransactionCommit { .. } => {
                // Transactions are handled locally
            }
            WALOperation::TransactionAbort { .. } => {
                // Transactions are handled locally
            }
            WALOperation::Checkpoint { .. } => {
                // Checkpoints are handled locally
            }
            WALOperation::BeginTransaction { .. } => {
                // Transactions are handled locally
            }
            WALOperation::CommitTransaction { .. } => {
                // Transactions are handled locally
            }
            WALOperation::AbortTransaction { .. } => {
                // Transactions are handled locally
            }
        }
        Ok(())
    }

    pub fn replicate_operation(&self, operation: WALOperation) -> Result<()> {
        if self.config.role == ReplicationRole::Master {
            self.pending_operations.write().push_back(operation);

            // Notify sync thread
            let (lock, cvar) = &*self.sync_condvar;
            let mut pending = lock.lock().unwrap();
            *pending = true;
            cvar.notify_one();
        }

        Ok(())
    }

    pub fn get_replication_lag(&self) -> u64 {
        if self.config.role == ReplicationRole::Slave {
            self.last_synced_lsn.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn is_synced(&self) -> bool {
        if self.config.role == ReplicationRole::Master {
            self.pending_operations.read().is_empty()
        } else {
            true // Slaves are always considered synced
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LightningDbConfig;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_replication_config() {
        let config = ReplicationConfig::default();
        assert_eq!(config.role, ReplicationRole::Master);
        assert_eq!(config.bind_address, "127.0.0.1:7890");
        assert_eq!(config.sync_interval_ms, 100);
        assert_eq!(config.heartbeat_interval_ms, 5000);
        assert_eq!(config.snapshot_threshold, 10000);
        assert_eq!(
            config.conflict_resolution,
            ConflictResolution::LastWriteWins
        );
    }

    #[test]
    fn test_master_slave_setup() {
        let master_dir = tempdir().unwrap();
        let slave_dir = tempdir().unwrap();

        // Create master database
        let master_config = LightningDbConfig::default();
        let master_db =
            Arc::new(Database::create(master_dir.path().join("master.db"), master_config).unwrap());

        // Create slave database
        let slave_config = LightningDbConfig::default();
        let slave_db =
            Arc::new(Database::create(slave_dir.path().join("slave.db"), slave_config).unwrap());

        // Setup master replication
        let master_repl_config = ReplicationConfig {
            role: ReplicationRole::Master,
            bind_address: "127.0.0.1:17890".to_string(),
            ..Default::default()
        };

        let master_repl = ReplicationManager::new(master_repl_config, master_db.clone());

        // Setup slave replication
        let slave_repl_config = ReplicationConfig {
            role: ReplicationRole::Slave,
            bind_address: "127.0.0.1:17891".to_string(),
            master_address: Some("127.0.0.1:17890".to_string()),
            ..Default::default()
        };

        let slave_repl = ReplicationManager::new(slave_repl_config, slave_db.clone());

        // Start replication
        master_repl.start().unwrap();
        thread::sleep(Duration::from_millis(100)); // Let master start
        slave_repl.start().unwrap();

        // Write to master
        master_db.put(b"key1", b"value1").unwrap();
        master_repl
            .replicate_operation(WALOperation::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            })
            .unwrap();

        // Wait for replication
        thread::sleep(Duration::from_millis(500));

        // Verify slave has the data
        let value = slave_db.get(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Cleanup
        master_repl.stop();
        slave_repl.stop();
    }

    #[test]
    fn test_enhanced_replication() {
        let dir = tempdir().unwrap();
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(dir.path().join("test.db"), config).unwrap());

        let repl_config = ReplicationConfig::default();
        let manager = EnhancedReplicationManager::new(repl_config, db.clone());

        // Test statistics
        let stats = manager.get_stats();
        assert_eq!(stats.role, ReplicationRole::Master);
        assert!(!stats.is_running);
        assert_eq!(stats.current_lsn, 0);
        assert_eq!(stats.pending_operations, 0);

        // Test operation replication
        let op = WALOperation::Put {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };

        manager.replicate_operation(op).unwrap();

        let stats = manager.get_stats();
        assert_eq!(stats.current_lsn, 1);
        assert_eq!(stats.pending_operations, 1);
    }
}
